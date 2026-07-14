import axios from "axios";
import logger from "./logger.js";

let zohoAccessToken = null;
export async function getZohoAccessToken() {
  if (zohoAccessToken) return zohoAccessToken;

  try {
    const res = await axios.post(
      "https://accounts.zoho.com/oauth/v2/token",
      null,
      {
        params: {
          refresh_token: process.env.ZOHO_REFRESH_TOKEN,
          client_id: process.env.ZOHO_CLIENT_ID,
          client_secret: process.env.ZOHO_CLIENT_SECRET,
          grant_type: "refresh_token",
        },
      },
    );

    if (!res.data.access_token) {
      throw new Error(
        `Failed to refresh Zoho token: ${JSON.stringify(res.data)}`,
      );
    }

    logger.info("Zoho access token refreshed successfully", {
      expiresIn: res.data.expires_in,
      apiDomain: res.data.api_domain,
    });

    zohoAccessToken = res.data.access_token;

    setTimeout(
      () => {
        zohoAccessToken = null;
      },
      (res.data.expires_in - 60) * 1000,
    );

    return zohoAccessToken;
  } catch (err) {
    console.error(
      "Zoho token refresh error:",
      err.response?.data || err.message,
    );
    throw err;
  }
}
