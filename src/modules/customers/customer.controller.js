import pool from "../../db/pool.js";
import axios from "axios";
import fs from "fs"
import FormData from "form-data";
import { v4 as uuidv4 } from "uuid";
import path from "path";
let zohoAccessToken = null;
const ORG_ID = process.env.ZOHO_BOOKS_ORG_ID;  // Unused—consider removing or use in params

export async function getZohoAccessToken() {
  if (zohoAccessToken) return zohoAccessToken;

  try {
    const res = await axios.post(
      "https://accounts.zoho.com/oauth/v2/token",  // Adjust to .eu/.in if non-US
      null,
      {
        params: {
          refresh_token: process.env.ZOHO_REFRESH_TOKEN,
          client_id: process.env.ZOHO_CLIENT_ID,
          client_secret: process.env.ZOHO_CLIENT_SECRET,
          grant_type: "refresh_token",
        },
      }
    );

    if (!res.data.access_token) {
      throw new Error(`Failed to refresh Zoho token: ${JSON.stringify(res.data)}`);
    }

    console.log("Fetched new Zoho access token:", res.data);
    zohoAccessToken = res.data.access_token;

    // Reset token ~1 minute before expiry
    setTimeout(() => {
      zohoAccessToken = null;
    }, (res.data.expires_in - 60) * 1000);

    return zohoAccessToken;
  } catch (err) {
    console.error("Zoho token refresh error:", err.response?.data || err.message);
    throw err;
  }
}

// Global sync flag (module-level)
let isSyncRunning = false;


export async function getCustomersPanel(req, res) {
  // console.log("Received request for customer panel sync");

  try {
    const { search = 'All', limit = 6000 } = req.query;

    if (isSyncRunning) {
      // console.log("Customer sync already running, returning cached data...");
      const { rows } = await pool.query(
        "SELECT * FROM customers ORDER BY created_time DESC LIMIT $1",
        [parseInt(limit)]
      );
      return res.json(rows);
    }

    isSyncRunning = true;
    // console.log("🔄 Starting Zoho Books customer sync with pagination...");

    const token = await getZohoAccessToken();
    // console.log("Fetched Zoho access token successfully");

    let page = 1;
    const per_page = 200;
    let allCustomers = [];

    let pageContext;
    do {
      // console.log(`Fetching page ${page}...`);

      try {
        const zohoRes = await axios.get(
          "https://www.zohoapis.com/books/v3/contacts",
          {
            headers: {
              Authorization: `Zoho-oauthtoken ${token}`,
              "Content-Type": "application/json",
            },
            params: {
              organization_id: process.env.ZOHO_BOOKS_ORG_ID,
              contact_type: 'customer',
              page: page,
              per_page: per_page,
              sort_column: 'created_time',   // Newest first
              sort_order: 'D'                // Descending
            },
          }
        );

        // Basic response validation
        if (zohoRes.data.code !== 0) {
          throw new Error(`Zoho API error: ${zohoRes.data.message || 'Unknown error'}`);
        }

        const contacts = zohoRes.data.contacts || [];
        allCustomers.push(...contacts);

        pageContext = zohoRes.data.page_context;
        // console.log(
        //   `Page ${page}: Fetched ${contacts.length} customers. ` +
        //   `Total so far: ${allCustomers.length}. Has more: ${pageContext?.has_more_page}`
        // );

        page++;

        // Rate limit safety: ~85 requests per minute (Zoho allows ~100/min)
        await new Promise(resolve => setTimeout(resolve, 700));

      } catch (pageErr) {
        console.error(`Error fetching page ${page}:`, {
          message: pageErr.message,
          response: pageErr.response?.data,
          status: pageErr.response?.status,
        });
        // You can decide: break early or continue with what we have
        // For now: break to avoid endless loop on persistent error
        break;
      }
    } while (pageContext?.has_more_page === true);

    // console.log(`───────────────────────────────────────────────`);
    // console.log(`Sync complete. Total pages processed: ${page - 1}`);
    // console.log(`Total customers fetched from Zoho: ${allCustomers.length}`);
    // console.log(`───────────────────────────────────────────────`);

    // Sync to DB
    for (const c of allCustomers) {
      if (!c.contact_id) continue;

      const addressStr = c.billing_address ? JSON.stringify(c.billing_address) : null;
      const createdTime = c.created_time ? new Date(c.created_time).toISOString() : null;
      const modifiedTime = c.last_modified_time ? new Date(c.last_modified_time).toISOString() : null;

      await pool.query(
        `INSERT INTO customers 
          (zoho_id, contact_name, email, address, zoho_notes, associated_by, 
           system_notes, contact_type, status, created_by, modified_by, created_time, modified_time)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT (zoho_id) DO UPDATE SET
           contact_name = COALESCE(EXCLUDED.contact_name, customers.contact_name),
           email = COALESCE(EXCLUDED.email, customers.email),
           address = COALESCE(EXCLUDED.address, customers.address),
           zoho_notes = COALESCE(EXCLUDED.zoho_notes, customers.zoho_notes),
           associated_by = COALESCE(EXCLUDED.associated_by, customers.associated_by),
           system_notes = COALESCE(EXCLUDED.system_notes, customers.system_notes),
           contact_type = COALESCE(EXCLUDED.contact_type, customers.contact_type),
           status = COALESCE(EXCLUDED.status, customers.status),
           created_by = COALESCE(EXCLUDED.created_by, customers.created_by),
           modified_by = COALESCE(EXCLUDED.modified_by, customers.modified_by),
           created_time = COALESCE(EXCLUDED.created_time, customers.created_time),
           modified_time = COALESCE(EXCLUDED.modified_time, customers.modified_time)`,
        [
          c.contact_id,
          c.contact_name || null,
          c.email || null,
          addressStr,
          c.notes || null,
          null,  // associated_by
          null,  // system_notes
          c.contact_type || null,
          c.status === "active",
          c.created_by_name || null,
          c.custom_fields?.find(cf => cf.label === "Updated By")?.value || null,
          createdTime,
          modifiedTime,
        ]
      );
    }

    // Return latest from DB
    const limitQuery = parseInt(limit) > 0 ? `LIMIT $1` : '';
    const { rows } = await pool.query(
      `SELECT * FROM customers ORDER BY created_time DESC ${limitQuery}`,
      limitQuery ? [parseInt(limit)] : []
    );

    res.json(rows);

  } catch (err) {
    console.error("Zoho Books sync error:", {
      message: err.message,
      response: err.response?.data,
      status: err.response?.status,
    });
    const errorMessage = err.response?.data?.message || err.message;
    const statusCode = err.response?.status || 500;
    res.status(statusCode).json({
      error: `Failed to sync customers: ${errorMessage}`,
    });
  } finally {
    isSyncRunning = false;
    console.log("✅ Customer sync finished");
  }
}
// getCustomers remains the same (DB query with search/limit for efficiency)
export async function getCustomers(req, res) {
  try {
    const { search = '', limit = 50 } = req.query;

    let query = "SELECT * FROM customers WHERE 1=1";
    let params = [];

    if (search.trim() && search !== 'All') {  // Ignore 'All' for full list
      query += " AND (contact_name ILIKE $1 OR email ILIKE $1)";
      params.push(`%${search.trim()}%`);
    }

    query += ` ORDER BY created_time DESC LIMIT $${params.length + 1}`;
    params.push(parseInt(limit));
    console.log('query limits and search', query, params
    )
    const { rows } = await pool.query(query, params);
    // console.log('row data',rows)
    res.json(rows);
  } catch (err) {
    console.error("Error fetching customers:", err);
    res.status(500).json({ error: 'Failed to fetch customers' });
  }
}
export async function deleteCustomer(req, res) {
  const { zoho_id } = req.params; // we only need zoho_id for contacts
  const token = await getZohoAccessToken();

  try {
    // Delete from Zoho
    const zohoRes = await axios.delete(
      `https://www.zohoapis.com/books/v3/contacts/${zoho_id}`,
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          "Content-Type": "application/json",
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );

    // Delete from local DB
    const { rows } = await pool.query(
      `DELETE FROM customers WHERE zoho_id = $1 RETURNING zoho_id, contact_name, contact_type`,
      [zoho_id]
    );

    if (!rows.length) {
      return res.status(404).json({ error: "Customer not found in local DB" });
    }

    res.status(200).json({
      message: "Customer deleted successfully",
      deleted: rows[0],
      zoho_response: zohoRes.data,
    });
  } catch (err) {
    console.error("Delete contact failed:", err.response?.data || err.message);
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || "Failed to delete contact",
    });
  }
}


export async function saveContacts(req, res) {
  const { zoho_id, contacts } = req.body;

  // Input validation
  if (!zoho_id) {
    console.error("Missing zoho_id in request body");
    return res.status(400).json({ error: "zoho_id is required" });
  }
  if (!Array.isArray(contacts) || contacts.length === 0) {
    console.error("Invalid or empty contacts array");
    return res.status(400).json({ error: "Contacts array is required and must not be empty" });
  }

  try {
    const token = await getZohoAccessToken();
    if (!token) {
      console.error("Failed to retrieve Zoho access token");
      throw new Error("Failed to retrieve Zoho access token");
    }
    console.log(`Zoho access token: ${token}`);

    // Verify zoho_id exists in Zoho Books
    try {
      const zohoResponse = await axios.get(
        `https://www.zohoapis.com/books/v3/contacts/${zoho_id}`,
        {
          headers: {
            Authorization: `Zoho-oauthtoken ${token}`,
            "Content-Type": "application/json",
          },
          params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
        }
      );
      console.log(`Verified zoho_id: ${zoho_id}`, zohoResponse.data.contact);
    } catch (err) {
      console.error(`Invalid zoho_id: ${zoho_id}`, err.response?.data || err.message);
      return res.status(400).json({
        error: `Invalid zoho_id: ${zoho_id}`,
        details: err.response?.data || err.message,
      });
    }

    // Check if contact persons API is restricted
    let isApiRestricted = false;
    try {
      // Test POST request with minimal payload
      await axios.post(
        `https://www.zohoapis.com/books/v3/contacts/${zoho_id}/contactpersons`,
        { first_name: "test" },
        {
          headers: {
            Authorization: `Zoho-oauthtoken ${token}`,
            "Content-Type": "application/json",
          },
          params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
        }
      );
    } catch (testErr) {
      if (testErr.response?.status === 405 && testErr.response?.data?.code === 37) {
        console.warn("Contact persons API is restricted", testErr.response.data);
        isApiRestricted = true;
      }
    }

    if (isApiRestricted) {
      console.error("Contact persons API is restricted, likely due to trial account");
      return res.status(400).json({
        error: "Creating or updating contact persons is not allowed, likely due to Zoho Books trial account restrictions",
        details: { code: 37, message: "Contact persons API is restricted" },
      });
    }

    const updatedContacts = [];
    for (const contact of contacts) {
      const payload = {
        first_name: contact.name.split(" ")[0] || "",
        last_name: contact.name.split(" ").slice(1).join(" ") || "",
        phone: contact.phone || ""
      };

      if (process.env.ZOHO_ALLOW_EMAIL === "true" && contact.email) {
        payload.email = contact.email;
      }

      let response;
      try {
        if (contact.isNew) {
          response = await axios.post(
            `https://www.zohoapis.com/books/v3/contacts/${zoho_id}/contactpersons`,
            payload,
            { headers, params }
          );
        } else {
          try {
            response = await axios.put(
              `https://www.zohoapis.com/books/v3/contacts/${zoho_id}/contactpersons/${contact.id}`,
              payload,
              { headers, params }
            );
          } catch (err) {
            if (err.response?.status === 404) {
              // fallback to create
              response = await axios.post(
                `https://www.zohoapis.com/books/v3/contacts/${zoho_id}/contactpersons`,
                payload,
                { headers, params }
              );
            } else throw err;
          }
        }
      } catch (err) {
        console.error("Zoho contact save failed", err.response?.data || err.message);
        throw err;
      }

      const cp = response.data.contact_person;
      updatedContacts.push({
        id: cp.contact_person_id,
        name: [cp.first_name, cp.last_name].filter(Boolean).join(" ") || null,
        phone: cp.phone || null,
        email: cp.email || null,
      });
    }


    console.log("Contacts saved successfully:", updatedContacts);
    res.status(200).json(updatedContacts);
  } catch (err) {
    console.error("Save contacts failed:", {
      message: err.message,
      response: err.response?.data,
      status: err.response?.status,
      zoho_id,
      contacts,
    });
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || "Failed to save contacts",
      details: err.response?.data || err.message,
    });
  }
}
export async function deleteContact(req, res) {
  const { zoho_id, contact_person_id } = req.params;
  const token = await getZohoAccessToken();

  try {
    await axios.delete(
      `https://www.zohoapis.com/books/v3/contacts/${zoho_id}/contactpersons/${contact_person_id}`,
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          "Content-Type": "application/json",
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );
    res.status(200).json({ message: "Contact deleted successfully" });
  } catch (err) {
    console.error("Delete contact failed:", err.response?.data || err.message);
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || "Failed to delete contact",
    });
  }
}

export async function uploadDocument(req, res) {
  const { zoho_id } = req.body;
  const file = req.file;

  if (!zoho_id) {
    console.error('Missing zoho_id in request body');
    return res.status(400).json({ error: 'zoho_id is required' });
  }
  if (!file) {
    console.error('No file uploaded in request');
    return res.status(400).json({ error: 'File is required' });
  }

  console.log('Received file:', {
    path: file.path,
    originalname: file.originalname,
    mimetype: file.mimetype,
    size: file.size,
    zoho_id,
  });

  try {
    await fs.promises.access(file.path, fs.constants.R_OK);
    console.log(`File accessible at: ${file.path}`);
  } catch (err) {
    console.error(`File not accessible at ${file.path}:`, err);
    return res.status(500).json({ error: 'File not accessible', details: err.message });
  }

  try {
    const token = await getZohoAccessToken();
    if (!token) {
      console.error('Failed to retrieve Zoho access token');
      throw new Error('Failed to retrieve Zoho access token');
    }

    console.log(`Verifying contact_id: ${zoho_id}`);
    try {
      const contactResponse = await axios.get(
        `https://www.zohoapis.com/books/v3/contacts/${zoho_id}`, // Update to .eu or your region
        {
          headers: {
            Authorization: `Zoho-oauthtoken ${token}`,
            'Content-Type': 'application/json',
          },
          params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
        }
      );
      console.log(`Contact verified:`, contactResponse.data.contact);
    } catch (err) {
      console.error(`Invalid contact_id: ${zoho_id}`, err.response?.data || err.message);
      return res.status(400).json({
        error: `Invalid contact_id: ${zoho_id}`,
        details: err.response?.data || err.message,
      });
    }

    console.log(`Uploading document with contact_id: ${zoho_id}`);
    const formData = new FormData();
    const fileBuffer = await fs.promises.readFile(file.path);
    formData.append('document', fileBuffer, {
      filename: file.originalname,
      contentType: file.mimetype,
    });
    formData.append('contact_id', zoho_id);
    formData.append('document_name', file.originalname);

    const response = await axios.post(
      `https://www.zohoapis.com/books/v3/documents`, // Update to .eu or your region
      formData,
      {
        headers: {
          ...formData.getHeaders(),
          Authorization: `Zoho-oauthtoken ${token}`,
        },
        params: {
          organization_id: process.env.ZOHO_BOOKS_ORG_ID,
          contact_id: zoho_id, // Add as query parameter
        },
      }
    );

    console.log('Zoho API full response:', JSON.stringify(response.data, null, 2));

    if (!response.data.documents) {
      console.error('Unexpected response structure:', response.data);
      throw new Error("Zoho API response does not contain 'documents' object");
    }

    const document = response.data.documents;
    console.log(`Uploaded document contact_id: ${document.contact_id || 'not set'}`);

    // Verify association by fetching document
    const verifyResponse = await axios.get(
      `https://www.zohoapis.com/books/v3/documents/${document.document_id}`,
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          'Content-Type': 'application/json',
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );
    console.log('Document verification response:', JSON.stringify(verifyResponse.data, null, 2));

    console.log(`File retained at: ${file.path}`);

    res.status(200).json({
      document_id: document.document_id,
      file_name: document.file_name,
      file_type: document.document_type || 'document',
      file_size_formatted: document.file_size_formatted || `${(file.size / 1024).toFixed(2)} KB`,
      uploaded_on_date_formatted: document.created_time
        ? new Date(document.created_time).toLocaleDateString()
        : new Date().toLocaleDateString(),
      uploaded_by: document.uploaded_by || 'User',
    });
  } catch (err) {
    console.error('Upload document failed:', {
      message: err.message,
      response: err.response?.data,
      status: err.response?.status,
      zoho_id,
      filename: file?.originalname,
    });
    console.log(`File retained on error at: ${file?.path}`);
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || 'Failed to upload document',
      details: err.response?.data || err.message,
    });
  }
}

export async function getDocuments(req, res) {
  const { zoho_id } = req.params;

  if (!zoho_id) {
    console.error('Missing zoho_id in request');
    return res.status(400).json({ error: 'zoho_id is required' });
  }

  try {
    const token = await getZohoAccessToken();
    if (!token) {
      console.error('Failed to retrieve Zoho access token');
      throw new Error('Failed to retrieve Zoho access token');
    }

    console.log(`Fetching documents for zoho_id: ${zoho_id}, organization_id: ${process.env.ZOHO_BOOKS_ORG_ID}`);

    const response = await axios.get(
      `https://www.zohoapis.com/books/v3/documents`, // Update to .eu or your region
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          'Content-Type': 'application/json',
        },
        params: {
          organization_id: process.env.ZOHO_BOOKS_ORG_ID,
          contact_id: zoho_id,
        },
      }
    );

    console.log('Zoho API documents response:', JSON.stringify(response.data, null, 2));

    const documents = (response.data.documents || []).map((doc) => ({
      document_id: doc.document_id,
      file_name: doc.file_name,
      file_type: doc.document_type || 'document',
      file_size_formatted: doc.file_size_formatted || `${(doc.file_size / 1024).toFixed(2)} KB`,
      uploaded_on_date_formatted: doc.created_time
        ? new Date(doc.created_time).toLocaleDateString()
        : new Date().toLocaleDateString(),
      uploaded_by: doc.uploaded_by || 'User',
    }));

    console.log('Mapped documents:', documents);
    res.status(200).json(documents);
  } catch (err) {
    console.error('Error fetching documents:', {
      message: err.message,
      response: err.response?.data,
      status: err.response?.status,
      zoho_id,
    });
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || 'Failed to fetch documents',
      details: err.response?.data || err.message,
    });
  }
}


export async function updateDocument(req, res) {
  const { zoho_id, document_id } = req.params;
  const { document_name } = req.body;

  if (!zoho_id || !document_id || !document_name) {
    console.error('Missing required parameters', { zoho_id, document_id, document_name });
    return res.status(400).json({ error: 'zoho_id, document_id, and document_name are required' });
  }

  try {
    const token = await getZohoAccessToken();
    if (!token) {
      console.error('Failed to retrieve Zoho access token');
      throw new Error('Failed to retrieve Zoho access token');
    }

    console.log(`Updating document ${document_id} for zoho_id: ${zoho_id}`);
    const response = await axios.put(
      `https://www.zohoapis.com/books/v3/documents/${document_id}`, // Update to .eu or your region
      { document_name },
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          'Content-Type': 'application/json',
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );

    console.log('Zoho API update response:', JSON.stringify(response.data, null, 2));

    const document = response.data.document;
    res.status(200).json({
      document_id: document.document_id,
      file_name: document.file_name,
      file_type: document.document_type || 'document',
      file_size_formatted: document.file_size_formatted || `${(document.file_size / 1024).toFixed(2)} KB`,
      uploaded_on_date_formatted: document.created_time
        ? new Date(document.created_time).toLocaleDateString()
        : new Date().toLocaleDateString(),
      uploaded_by: document.uploaded_by || 'User',
    });
  } catch (err) {
    console.error('Update document failed:', {
      message: err.message,
      response: err.response?.data,
      status: err.response?.status,
      zoho_id,
      document_id,
    });
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || 'Failed to update document',
      details: err.response?.data || err.message,
    });
  }
}


export async function downloadDocument(req, res) {
  const { zoho_id, document_id } = req.params;

  try {
    const token = await getZohoAccessToken();
    if (!token) {
      throw new Error('Failed to retrieve Zoho access token');
    }

    // Fetch document metadata
    const response = await axios.get(
      `https://www.zohoapis.com/books/v3/documents/${document_id}`,
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          'Content-Type': 'application/json',
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );

    const document = response.data.document;
    const fileName = document.file_name;

    // Assuming files are retained in uploads folder
    const filePath = path.join(process.cwd(), 'uploads', `${zoho_id}-${fileName}`);
    try {
      await fs.promises.access(filePath, fs.constants.R_OK);
      res.download(filePath, fileName);
    } catch (err) {
      console.error(`File not found: ${filePath}`, err);
      res.status(404).json({ error: 'File not found on server' });
    }
  } catch (err) {
    console.error('Error downloading document:', err);
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || 'Failed to download document',
      details: err.response?.data || err.message,
    });
  }
}
export async function deleteDocument(req, res) {
  const { zoho_id, document_id } = req.params;

  if (!zoho_id || !document_id) {
    console.error('Missing required parameters', { zoho_id, document_id });
    return res.status(400).json({ error: 'zoho_id and document_id are required' });
  }

  try {
    const token = await getZohoAccessToken();
    if (!token) {
      console.error('Failed to retrieve Zoho access token');
      throw new Error('Failed to retrieve Zoho access token');
    }

    console.log(`Deleting document ${document_id} for zoho_id: ${zoho_id}`);
    await axios.delete(
      `https://www.zohoapis.com/books/v3/documents/${document_id}`, // Update to .eu or your region
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          'Content-Type': 'application/json',
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );

    res.status(200).json({ message: 'Document deleted successfully' });
  } catch (err) {
    console.error('Delete document failed:', {
      message: err.message,
      response: err.response?.data,
      status: err.response?.status,
      zoho_id,
      document_id,
    });
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || 'Failed to delete document',
      details: err.response?.data || err.message,
    });
  }
}
// Helper function (add outside the function, e.g., at module level)
function isEmptyAddress(addrObj) {
  if (!addrObj || typeof addrObj !== 'object') return true;
  const fields = ['attention', 'address', 'street2', 'city', 'state_code', 'state', 'zip', 'country', 'county', 'country_code', 'phone', 'fax'];
  return fields.every(field => !addrObj[field] || addrObj[field].trim() === '');
}

export async function updateCustomer(req, res) {
  console.log("Received request to update customer:", req.body);
  const { zoho_id } = req.params;
  const {
    contact_name, email, address, zoho_notes, associated_by, system_notes, type,
    contact_type: bodyContactType  // Rename to avoid confusion
  } = req.body;

  try {
    // Fetch current from DB
    const { rows } = await pool.query("SELECT * FROM customers WHERE zoho_id = $1", [zoho_id]);
    if (rows.length === 0) {
      return res.status(404).json({ error: "Customer not found" });
    }
    const currentCustomer = rows[0];
    console.log("Current DB customer:", { zoho_id, contact_type: currentCustomer.contact_type, type: currentCustomer.type });

    // Check for Zoho-updatable changes (exclude local fields)
    const zohoChanges = { contact_name, email, zoho_notes, address };
    const hasZohoChanges = Object.values(zohoChanges).some(v => v !== undefined && v !== '' && v !== null);

    let zohoCustomer = { ...currentCustomer };  // Start with current
    if (hasZohoChanges) {
      const token = await getZohoAccessToken();

      // Build partial payload: NO contact_type!
      const payload = {};
      if (contact_name !== undefined && contact_name !== '') {
        payload.contact_name = contact_name;
      }
      if (email !== undefined && email !== '') {
        payload.primary_email = [{ email_address: email, is_primary: true }];
      }
      if (zoho_notes !== undefined && zoho_notes !== '') {
        payload.notes = zoho_notes;
      }
      if (address !== undefined && address !== '' && address !== null) {
        try {
          const addrObj = typeof address === 'string' ?
            (address.startsWith('{') ? JSON.parse(address) : { address1: address }) :
            address;
          payload.billing_address = addrObj;
        } catch (e) {
          console.warn("Invalid address format, skipping Zoho update for address:", e.message);
        }
      }

      // Only call Zoho if payload has at least one field
      if (Object.keys(payload).length > 0) {
        console.log("Updating Zoho with payload:", payload);
        const zohoRes = await axios.put(
          `https://www.zohoapis.com/books/v3/contacts/${zoho_id}`,
          payload,
          {
            headers: {
              Authorization: `Zoho-oauthtoken ${token}`,
              "Content-Type": "application/json",
            },
            params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
          }
        );

        zohoCustomer = { ...currentCustomer, ...zohoRes.data.contact };  // Merge updated fields
        console.log("Zoho updated:", { contact_id: zohoCustomer.contact_id, contact_type: zohoCustomer.contact_type });
      } else {
        console.log("No valid Zoho changes; skipping API call");
      }
    } else {
      console.log("Only local changes (type, associated_by, system_notes); skipping Zoho");
    }

    // Fixed: Check for empty address before stringifying
    let addressStr = null;
    if (zohoCustomer.billing_address && !isEmptyAddress(zohoCustomer.billing_address)) {
      addressStr = JSON.stringify(zohoCustomer.billing_address);
    } else if (address && !isEmptyAddress(typeof address === 'string' ? JSON.parse(address) : address)) {
      addressStr = typeof address === 'string' ? address : JSON.stringify(address);
    } else {
      addressStr = currentCustomer.address || null;  // Preserve existing if meaningful
    }

    // Timestamps: Pull from Zoho or default
    const createdTime = zohoCustomer.created_time || currentCustomer.created_time || null;
    const modifiedTime = zohoCustomer.last_modified_time || new Date().toISOString();

    const { rows: updatedRows } = await pool.query(
      `INSERT INTO customers 
        (zoho_id, contact_name, email, address, zoho_notes, associated_by, 
         system_notes, type, contact_type, status, created_by, modified_by, created_time, modified_time)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
       ON CONFLICT (zoho_id) DO UPDATE SET
         contact_name = COALESCE(EXCLUDED.contact_name, customers.contact_name),
         email = COALESCE(EXCLUDED.email, customers.email),
         address = COALESCE(EXCLUDED.address, customers.address),
         zoho_notes = COALESCE(EXCLUDED.zoho_notes, customers.zoho_notes),
         associated_by = COALESCE(EXCLUDED.associated_by, customers.associated_by),
         system_notes = COALESCE(EXCLUDED.system_notes, customers.system_notes),
         type = COALESCE(EXCLUDED.type, customers.type),  -- Local update
         contact_type = customers.contact_type,  -- ALWAYS preserve Zoho-linked type
         status = COALESCE(EXCLUDED.status, customers.status),
         created_by = COALESCE(EXCLUDED.created_by, customers.created_by),
         modified_by = COALESCE(EXCLUDED.modified_by, customers.modified_by),
         created_time = COALESCE(EXCLUDED.created_time, customers.created_time),
         modified_time = COALESCE(EXCLUDED.modified_time, customers.modified_time)
       RETURNING *`,
      [
        zoho_id,
        zohoCustomer.contact_name || contact_name || currentCustomer.contact_name || null,
        zohoCustomer.primary_email?.[0]?.email_address || email || currentCustomer.email || null,
        addressStr,  // Now null for empty
        zohoCustomer.notes || zoho_notes || currentCustomer.zoho_notes || null,
        associated_by !== undefined ? (associated_by || null) : currentCustomer.associated_by,
        system_notes !== undefined ? (system_notes || null) : currentCustomer.system_notes,
        type || currentCustomer.type || null,
        currentCustomer.contact_type,
        zohoCustomer.status === "active" || currentCustomer.status,
        zohoCustomer.created_by_name || currentCustomer.created_by || null,
        zohoCustomer.updated_by_name || currentCustomer.modified_by || null,
        createdTime,
        modifiedTime,
      ]
    );

    console.log("Updated local customer record:", updatedRows[0]);
    res.json(updatedRows[0]);

  } catch (err) {
    // ... (fallback and error handling unchanged)
    console.error("Update customer failed:", {
      code: err.response?.data?.code,
      message: err.response?.data?.message || err.message,
      status: err.response?.status,
    });

    // Fallback: If Zoho error, update local fields in DB anyway
    if (err.response?.status >= 400 && (type !== undefined || associated_by !== undefined || system_notes !== undefined)) {
      console.log("Zoho failed; forcing local DB update");
      try {
        const { rows: fallbackRows } = await pool.query(
          `UPDATE customers SET 
             type = COALESCE($1::text, type),
             associated_by = COALESCE($2::text, associated_by),
             system_notes = COALESCE($3::text, system_notes),
             modified_by = COALESCE($4::text, modified_by),
             modified_time = NOW()
           WHERE zoho_id = $5
           RETURNING *`,
          [type, associated_by || null, system_notes || null, 'System Update', zoho_id]
        );
        console.log("Fallback DB update:", fallbackRows[0]);
        return res.json(fallbackRows[0]);
      } catch (fallbackErr) {
        console.error("Fallback DB update failed:", fallbackErr);
      }
    }

    const statusCode = err.response?.status || 500;
    res.status(statusCode).json({
      error: `Failed to update customer: ${err.response?.data?.message || err.message}`,
    });
  }
}
// Include createCustomer and getCustomerById from previous messages
export async function createCustomer(req, res) {
  console.log("Received request to create customer:", req.body);
  const { contact_name, email, address, zoho_notes, associated_by, system_notes, type } = req.body ?? {};

  try {
    if (!contact_name || contact_name.trim().length === 0) {
      return res.status(400).json({ error: "Customer name is required" });
    }

    const token = await getZohoAccessToken();
    const uniqueSuffix = Date.now();

    // Payload for Zoho API
    const payload = {
      contact_name: `${contact_name.trim()}`,
      company_name: contact_name.trim(),
      contact_type: req.body.contact_type || "customer", // Zoho requires this
      email: email
        ? `${email.split("@")[0]}.${uniqueSuffix}@${email.split("@")[1]}`
        : `test.${uniqueSuffix}@example.com`,
      notes: zoho_notes || "",
    };

    const zohoRes = await axios.post(
      "https://www.zohoapis.com/books/v3/contacts",
      payload,
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          "Content-Type": "application/json",
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );

    const zohoCustomer = zohoRes.data.contact;

    // Insert into DB (12 columns now)
    const { rows } = await pool.query(
      `INSERT INTO customers 
        (zoho_id, contact_name, email, address, zoho_notes, associated_by, 
         system_notes, type, contact_type, status, created_by, modified_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
       ON CONFLICT (zoho_id) DO UPDATE SET
         contact_name = COALESCE(EXCLUDED.contact_name, customers.contact_name),
         email = COALESCE(customers.email, EXCLUDED.email),
         address = COALESCE(customers.address, EXCLUDED.address),
         zoho_notes = COALESCE(EXCLUDED.zoho_notes, customers.zoho_notes),
         associated_by = COALESCE(customers.associated_by, EXCLUDED.associated_by),
         system_notes = COALESCE(customers.system_notes, EXCLUDED.system_notes),
         type = COALESCE(EXCLUDED.type, customers.type),
         contact_type = COALESCE(EXCLUDED.contact_type, customers.contact_type),
         status = COALESCE(EXCLUDED.status, customers.status),
         created_by = COALESCE(EXCLUDED.created_by, customers.created_by),
         modified_by = COALESCE(customers.modified_by, EXCLUDED.modified_by)
       RETURNING zoho_id, contact_name, email, address, zoho_notes, associated_by, 
                 system_notes, type, contact_type, status, created_by, modified_by`,
      [
        zohoCustomer.contact_id,
        zohoCustomer.contact_name || null,
        zohoCustomer.email || email || null,
        address || null,
        zohoCustomer.notes || zoho_notes || null,
        associated_by || null,
        system_notes || null,
        type || null, // <-- from frontend
        zohoCustomer.contact_type || "customer", // <-- from Zoho
        zohoCustomer.status === "active",
        zohoCustomer.created_by_name || req.user?.name || "System",
        zohoCustomer.updated_by_name || null,
      ]
    );
    console.log("Created new customer:", rows[0]);
    res.status(201).json(rows[0]);
  } catch (err) {
    console.error("Zoho API error:", err.response?.data || err.message);
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || "Failed to create customer",
    });
  }
}


export async function getCustomerById(req, res) {
  const { id } = req.params;

  try {
    const { rows } = await pool.query(
      `SELECT zoho_id, contact_name, email, address, zoho_notes, associated_by, 
              system_notes, type, contact_type, status, created_by, modified_by 
       FROM customers WHERE zoho_id = $1`,
      [id]
    );

    if (!rows.length) {
      return res.status(404).json({ error: "Customer not found" });
    }

    const customer = rows[0];
    const token = await getZohoAccessToken();
    const zohoRes = await axios.get(
      `https://www.zohoapis.com/books/v3/contacts/${customer.zoho_id}`,
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          "Content-Type": "application/json",
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );

    const zohoCustomer = zohoRes.data.contact;

    const contactPersons = (zohoCustomer.contact_persons || []).map((cp) => ({
      id: cp.contact_person_id,
      name: `${cp.first_name || ""} ${cp.last_name || ""}`.trim() || null,
      phone: cp.phone || null,
      email: cp.email || null,
    }));

    const { rows: updatedRows } = await pool.query(
      `INSERT INTO customers 
    (zoho_id, contact_name, email, address, zoho_notes, associated_by, 
     system_notes, type, contact_type, status, created_by, modified_by)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
   ON CONFLICT (zoho_id) DO UPDATE SET
     contact_name = COALESCE(EXCLUDED.contact_name, customers.contact_name),
     email = COALESCE(customers.email, EXCLUDED.email),
     address = COALESCE(customers.address, EXCLUDED.address),
     zoho_notes = COALESCE(EXCLUDED.zoho_notes, customers.zoho_notes),
     associated_by = COALESCE(customers.associated_by, EXCLUDED.associated_by),
     system_notes = COALESCE(customers.system_notes, EXCLUDED.system_notes),
     type = COALESCE(customers.type, EXCLUDED.type),
     contact_type = COALESCE(customers.contact_type, EXCLUDED.contact_type),
     status = COALESCE(EXCLUDED.status, customers.status),
     created_by = COALESCE(EXCLUDED.created_by, customers.created_by),
     modified_by = COALESCE(customers.modified_by, EXCLUDED.modified_by)
   RETURNING zoho_id, contact_name, email, address, zoho_notes, associated_by, 
             system_notes, type, contact_type, status, created_by, modified_by`,
      [
        zohoCustomer.contact_id,
        zohoCustomer.contact_name || null,
        zohoCustomer.email || null,
        zohoCustomer.custom_fields?.cf_address || null,
        zohoCustomer.notes || null,
        null,
        null,
        null, // type
        zohoCustomer.contact_type || "customer",
        zohoCustomer.status === "active",
        zohoCustomer.created_by_name || null,
        zohoCustomer.custom_fields?.cf_updated_by_name || zohoCustomer.updated_by_name || null,
      ]
    );
    console.log("Updated local customer record:", updatedRows[0]);
    res.json({
      ...updatedRows[0],
      contact_persons: contactPersons,
      documents: zohoCustomer.documents || [],
    });
  } catch (err) {
    console.error("Fetch customer by ID failed:", err.response?.data || err.message);
    res.status(err.response?.status || 500).json({
      error: err.response?.data?.message || "Failed to fetch customer",
    });
  }
}
