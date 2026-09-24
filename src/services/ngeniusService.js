import axios from "axios";

const NGENIUS_BASE_URL = process.env.NGENIUS_BASE_URL;
const NGENIUS_API_KEY = process.env.NGENIUS_API_KEY;
const NGENIUS_OUTLET_REF = process.env.NGENIUS_OUTLET_REF;

let cachedToken = null;
let tokenExpiresAt = 0;

async function getAccessToken() {
  if (cachedToken && Date.now() < tokenExpiresAt) {
    return cachedToken;
  }

  const { data } = await axios.post(
    `${NGENIUS_BASE_URL}/identity/auth/access-token`,
    {},
    {
      headers: {
        Authorization: `Basic ${NGENIUS_API_KEY}`,
        "Content-Type": "application/vnd.ni-identity.v1+json",
      },
    },
  );

  cachedToken = data.access_token;
  tokenExpiresAt = Date.now() + (data.expires_in - 60) * 1000;
  return cachedToken;
}

export async function createOrder({
  orderReference,
  amount,
  currencyCode,
  redirectUrl,
}) {
  const token = await getAccessToken();

  try {
    const { data } = await axios.post(
      `${NGENIUS_BASE_URL}/transactions/outlets/${NGENIUS_OUTLET_REF}/orders`,
      {
        action: "SALE",
        amount: {
          currencyCode,
          value: Math.round(amount * 100),
        },
        merchantOrderReference: orderReference,
        merchantAttributes: {
          redirectUrl,
          skipConfirmationPage: true,
        },
      },
      {
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/vnd.ni-payment.v2+json",
          Accept: "application/vnd.ni-payment.v2+json",
        },
      },
    );

    const paymentUrl = data._links["payment"].href;
    const orderReferenceId = data.reference;

    return { paymentUrl, orderReferenceId, raw: data };
  } catch (err) {
    console.error(
      "N-Genius createOrder validation error:",
      JSON.stringify(err.response?.data, null, 2),
    );
    throw err;
  }
}

export async function getOrderStatus(orderReferenceId) {
  const token = await getAccessToken();

  const { data } = await axios.get(
    `${NGENIUS_BASE_URL}/transactions/outlets/${NGENIUS_OUTLET_REF}/orders/${orderReferenceId}`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.ni-payment.v2+json",
      },
    },
  );

  const state = data._embedded?.payment?.[0]?.state;
  return { state, raw: data };
}
