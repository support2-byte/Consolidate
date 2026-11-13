import { Router } from "express";
import {
  autocompleteAddress,
  geocodePlace,
  reverseGeocode,
} from "./maps.controller.js";
import { requireAppAuth } from "../../../middleware/appAuthMiddleware.js";

const router = Router();

router.get("/autocomplete", requireAppAuth, autocompleteAddress);
router.get("/geocode", requireAppAuth, geocodePlace);
router.get("/reverse-geocode", requireAppAuth, reverseGeocode);

export default router;
