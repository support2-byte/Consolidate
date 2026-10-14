import { Client } from "@googlemaps/google-maps-services-js";
import logger from "../../services/logger.js";

const client = new Client({});
const KEY = process.env.GOOGLE_MAPS_SERVER_KEY;

export const autocompleteAddress = async (req, res) => {
  const { input } = req.query;
  if (!input || String(input).trim().length < 3) {
    return res.json({ success: true, predictions: [] });
  }

  try {
    const response = await client.placeAutocomplete({
      params: {
        input: String(input),
        key: KEY,
      },
    });

    return res.json({
      success: true,
      predictions: response.data.predictions.map((p) => ({
        placeId: p.place_id,
        description: p.description,
      })),
    });
  } catch (error) {
    logger.error("autocompleteAddress error", {
      error: error.response?.data || error.message,
    });
    return res.json({ success: true, predictions: [] });
  }
};

export const geocodePlace = async (req, res) => {
  const { placeId } = req.query;
  if (!placeId) {
    return res
      .status(400)
      .json({ success: false, message: "placeId is required" });
  }

  try {
    const response = await client.placeDetails({
      params: {
        place_id: String(placeId),
        key: KEY,
        fields: ["formatted_address", "geometry"],
      },
    });

    const result = response.data.result;
    if (!result?.geometry?.location) {
      return res
        .status(404)
        .json({ success: false, message: "Address not found" });
    }

    return res.json({
      success: true,
      address: {
        formattedAddress: result.formatted_address,
        lat: result.geometry.location.lat,
        lng: result.geometry.location.lng,
      },
    });
  } catch (error) {
    logger.error("geocodePlace error", {
      error: error.response?.data || error.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Unable to geocode address" });
  }
};

export const reverseGeocode = async (req, res) => {
  const { lat, lng } = req.query;
  if (!lat || !lng) {
    return res
      .status(400)
      .json({ success: false, message: "lat and lng are required" });
  }

  try {
    const response = await client.reverseGeocode({
      params: {
        latlng: `${lat},${lng}`,
        key: KEY,
      },
    });

    const result = response.data.results[0];
    if (!result) {
      return res
        .status(404)
        .json({ success: false, message: "Address not found" });
    }

    return res.json({
      success: true,
      address: {
        formattedAddress: result.formatted_address,
        lat: Number(lat),
        lng: Number(lng),
      },
    });
  } catch (error) {
    logger.error("reverseGeocode error", {
      error: error.response?.data || error.message,
    });
    return res
      .status(500)
      .json({ success: false, message: "Unable to reverse geocode" });
  }
};
