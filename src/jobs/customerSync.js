import cron from "node-cron";
import axios from "axios";
import pool from "../db/pool.js";
import { getZohoAccessToken } from "../services/getZohoAccessToken.js";

async function syncCustomersFromZoho() {
  console.log("Customer sync started");

  try {
    const token = await getZohoAccessToken();

    let page = 1;
    let totalUpdated = 0;

    while (true) {
      const zohoRes = await axios.get(
        "https://www.zohoapis.com/books/v3/contacts",
        {
          headers: {
            Authorization: `Zoho-oauthtoken ${token}`,
          },

          params: {
            organization_id: process.env.ZOHO_BOOKS_ORG_ID,
            page,
            per_page: 200,
            contact_type: "customer",
          },
        },
      );

      const customers = zohoRes.data.contacts || [];

      if (!customers.length) {
        break;
      }

      for (const zohoCustomer of customers) {
        await pool.query(
          `
        INSERT INTO customers
        (
          zoho_id,
          contact_name,
          email,
          phone_number,
          address,
          zoho_notes,
          type,
          contact_type,
          status,
          created_by,
          modified_by,
          modified_time
        )
        VALUES
        (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW()
        )
        ON CONFLICT(zoho_id)
        DO UPDATE SET
        contact_name = EXCLUDED.contact_name,
        email = EXCLUDED.email,
        phone_number = EXCLUDED.phone_number,
        address = EXCLUDED.address,
        zoho_notes = EXCLUDED.zoho_notes,
        contact_type = EXCLUDED.contact_type,
        status = EXCLUDED.status,
        modified_by = EXCLUDED.modified_by,
        modified_time = NOW()
        `,
          [
            zohoCustomer.contact_id,
            zohoCustomer.contact_name || null,
            zohoCustomer.email || null,
            zohoCustomer.phone || zohoCustomer.mobile || null,
            zohoCustomer.billing_address?.address || null,
            zohoCustomer.notes || null,
            null,
            zohoCustomer.contact_type || "customer",
            zohoCustomer.status === "active",
            zohoCustomer.created_by_name || null,
            zohoCustomer.last_modified_by_name || null,
          ],
        );
        totalUpdated++;
      }

      console.log(`Synced page ${page}`);

      page++;
      await new Promise((r) => setTimeout(r, 1000));
    }

    console.log(`Customer sync finished. Total: ${totalUpdated}`);
  } catch (error) {
    console.error(
      "Customer sync failed:",
      error.response?.data || error.message,
    );
  }
}

cron.schedule("0 0 * * *", syncCustomersFromZoho, {
  timezone: "Asia/Karachi",
});

export default syncCustomersFromZoho;
