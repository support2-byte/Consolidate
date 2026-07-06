import express from "express";
import {
  getBanks,
  getPaymentTypes,
  getVessels,
  createVessel,
  updateVessel,
  deleteVessel,
  getShippingLines,
  getCurrencies,
  createPaymentType,
  updatePaymentType,
  deletePaymentType,
  getCategories,
  createCategory,
  updateCategory,
  deleteCategory,
  getSubcategories,
  createSubcategory,
  updateSubcategory,
  deleteSubcategory,
  getPlaces,
  createPlace,
  updatePlace,
  deletePlace,
  getThirdParties,
  createThirdParty,
  deleteThirdParty,
  updateThirdParty,
  createBank,
  updateBank,
  deleteBank,
  getAllStatus,
  updateStatus,
  addNewStatus,
  deleteStatus,
  createBugReport,
  getBugReports,
  updateBugReport,
  deleteBugReport,
  getDashboardData,
} from "./options.controllers.js";
import { bugReportUpload } from "../../middleware/upload.js";

const router = express.Router();

router.get("/banks", getBanks);
router.get("/vessels", getVessels);
router.get("/shipping-lines", getShippingLines);
router.get("/currencies", getCurrencies);

router.get("/payment-types/crud", getPaymentTypes);
router.post("/payment-types", createPaymentType);
router.put("/payment-types/:id", updatePaymentType);
router.delete("/payment-types/:id", deletePaymentType);
router.get("/vessels/crud", getVessels);
router.post("/vessels", createVessel);
router.put("/vessels/:id", updateVessel);
router.delete("/vessels/:id", deleteVessel);
router.get("/categories/crud", getCategories);
router.post("/categories", createCategory);
router.put("/categories/:id", updateCategory);
router.delete("/categories/:id", deleteCategory);
router.get("/subcategories/crud", getSubcategories);
router.post("/subcategories", createSubcategory);
router.put("/subcategories/:id", updateSubcategory);
router.delete("/subcategories/:id", deleteSubcategory);
router.get("/places/crud", getPlaces);
router.post("/places", createPlace);
router.put("/places/:id", updatePlace);
router.delete("/places/:id", deletePlace);
router.get("/thirdParty/crud", getThirdParties);
router.post("/thirdParty", createThirdParty);
router.put("/thirdParty/:id", updateThirdParty);
router.delete("/thirdParty/:id", deleteThirdParty);
router.get("/banks/crud", getBanks);
router.post("/banks", createBank);
router.put("/banks/:id", updateBank);
router.delete("/banks/:id", deleteBank);

router.get("/allStatus", getAllStatus);
router.put("/updateStatus/:id", updateStatus);
router.post("/addStatus", addNewStatus);
router.delete("/deleteStatus/:id", deleteStatus);

router.post(
  "/bug-report",
  bugReportUpload.array("attachments", 3),
  createBugReport,
);
router.get("/bug-report", getBugReports);
router.put(
  "/bug-report/:id",
  bugReportUpload.array("attachments", 3),
  updateBugReport,
);
router.delete("/bug-report/:id", deleteBugReport);

router.get("/dashboard", getDashboardData);

export default router;
