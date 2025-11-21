import pool from "../../db/pool.js";
import axios from "axios";
import fs from "fs"
import FormData from "form-data";
import { v4 as uuidv4 } from "uuid";
import path from "path";
let zohoAccessToken = null
const ORG_ID = process.env.ZOHO_BOOKS_ORG_ID;
export async function getZohoAccessToken() {
  if (zohoAccessToken) return zohoAccessToken;

  try {
    const res = await axios.post(
      "https://accounts.zoho.com/oauth/v2/token", // or .uk depending on org
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
let isSyncRunning = false; // ✅ global flag

export async function getCustomers(req, res) {
  try {
    // If sync already running, just return cached DB data
    if (isSyncRunning) {
      console.log("Customer sync already running, returning cached data...");
      const { rows } = await pool.query("SELECT * FROM customers ORDER BY created_time DESC");
      return res.json(rows);
    }

    // Begin sync
    isSyncRunning = true;
    console.log("🔄 Starting Zoho Books customer sync...");

    const token = await getZohoAccessToken();
    console.log("Fetched Zoho access token:", { access_token: token });

    const zohoRes = await axios.get(
      "https://www.zohoapis.com/books/v3/contacts",
      {
        headers: {
          Authorization: `Zoho-oauthtoken ${token}`,
          "Content-Type": "application/json",
        },
        params: { organization_id: process.env.ZOHO_BOOKS_ORG_ID },
      }
    );

    const contacts = zohoRes.data.contacts || [];
    const customers = contacts.filter(c => c.contact_type === "customer");

    for (const c of customers) {
      if (!c.contact_id) continue;
      await pool.query(
        `INSERT INTO customers 
          (zoho_id, contact_name, email, address, zoho_notes, associated_by, 
           system_notes, contact_type, status, created_by, modified_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (zoho_id) DO UPDATE SET
           contact_name = COALESCE(EXCLUDED.contact_name, customers.contact_name),
           email = COALESCE(EXCLUDED.email, customers.email),
           address = COALESCE(customers.address, EXCLUDED.address),
           zoho_notes = COALESCE(EXCLUDED.zoho_notes, customers.zoho_notes),
           associated_by = COALESCE(customers.associated_by, EXCLUDED.associated_by),
           system_notes = COALESCE(customers.system_notes, EXCLUDED.system_notes),
           contact_type = COALESCE(EXCLUDED.contact_type, customers.contact_type),
           status = COALESCE(EXCLUDED.status, customers.status),
           created_by = COALESCE(EXCLUDED.created_by, customers.created_by),
           modified_by = COALESCE(customers.modified_by, EXCLUDED.modified_by)`,
        [
          c.contact_id,
          c.contact_name || null,
          c.email || null,
          null,
          c.notes || null,
          null,
          null,
          c.contact_type || null,
          c.status === "active",
          c.created_by_name || null,
          c.custom_fields?.cf_updated_by_name || c.updated_by_name || null,
        ]
      );
    }

    // ✅ After sync, return all from DB
    const { rows } = await pool.query("SELECT * FROM customers ORDER BY created_time DESC");
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
      error: `Failed to fetch contacts: ${errorMessage}`,
    });
  } finally {
    isSyncRunning = false; // ✅ always release lock
    console.log("✅ Customer sync finished");
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

export async function updateCustomer(req, res) {
  const { zoho_id } = req.params;
  const { contact_name, email, address, zoho_notes, associated_by, system_notes, type } = req.body;

  try {
    const token = await getZohoAccessToken();

    // Build Zoho payload (Zoho needs contact_type)
    const payload = {
      contact_name: contact_name || null,
      email: email || null,
      notes: zoho_notes || "",
      contact_type: "customer", // always required by Zoho
    };

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

    const zohoCustomer = zohoRes.data.contact;
    console.log("Zoho update response:", zohoCustomer);

    const { rows } = await pool.query(
      `INSERT INTO customers 
        (zoho_id, contact_name, email, address, zoho_notes, associated_by, 
         system_notes, type, contact_type, status, created_by, modified_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
       ON CONFLICT (zoho_id) DO UPDATE SET
         contact_name = COALESCE(EXCLUDED.contact_name, customers.contact_name),
         email = COALESCE(EXCLUDED.email, customers.email),
         address = COALESCE(EXCLUDED.address, customers.address),
         zoho_notes = COALESCE(EXCLUDED.zoho_notes, customers.zoho_notes),
         associated_by = COALESCE(EXCLUDED.associated_by, customers.associated_by),
         system_notes = COALESCE(EXCLUDED.system_notes, customers.system_notes),
         type = COALESCE(EXCLUDED.type, customers.type),
         contact_type = COALESCE(EXCLUDED.contact_type, customers.contact_type),
         status = COALESCE(EXCLUDED.status, customers.status),
         created_by = COALESCE(EXCLUDED.created_by, customers.created_by),
         modified_by = COALESCE(EXCLUDED.modified_by, customers.modified_by)
       RETURNING zoho_id, contact_name, email, address, zoho_notes, associated_by, 
                 system_notes, type, contact_type, status, created_by, modified_by`,
      [
        zohoCustomer.contact_id,
        contact_name || zohoCustomer.contact_name || null,
        email || zohoCustomer.email || null,
        address || null,
        zoho_notes || zohoCustomer.notes || null,
        associated_by || null,
        system_notes || null,
        type || null, // frontend
        zohoCustomer.contact_type || "customer", // Zoho
        zohoCustomer.status === "active",
        zohoCustomer.created_by_name || null,
        zohoCustomer.updated_by_name || null,
      ]
    );

    console.log("Updated customer:", rows[0]);
    res.json(rows[0]);
  } catch (err) {
    console.error("Update customer failed:", err.response?.data || err.message);
    const errorMessage = err.response?.data?.message || err.message;
    const statusCode = err.response?.status || 500;
    res.status(statusCode).json({
      error: `Failed to update customer: ${errorMessage}`,
    });
  }
}


// Include createCustomer and getCustomerById from previous messages
export async function createCustomer(req, res) {
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
      contact_type: "customer", // Zoho requires this
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
