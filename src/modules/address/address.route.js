import { Router } from "express";
import {
  autocompleteAddress,
  geocodePlace,
  reverseGeocode,
} from "./address.controller.js";

const router = Router();

router.get("/autocomplete", autocompleteAddress);
router.get("/geocode", geocodePlace);
router.get("/reverse-geocode", reverseGeocode);

export default router;
