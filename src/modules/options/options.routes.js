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
  toggleSendEmail,
  getModules,
  createCompany,
  getCompanies,
  updateCompany,
  createDocumentTemplate,
  getDocumentTemplates,
  updateDocumentTemplate,
} from "./options.controllers.js";
import { bugReportUpload, companyUpload } from "../../middleware/upload.js";
import { requireAuth } from "../../modules/auth/auth.middleware.js";

const router = express.Router();

router.get("/banks", requireAuth, getBanks);
router.get("/vessels", requireAuth, getVessels);
router.get("/shipping-lines", requireAuth, getShippingLines);
router.get("/currencies", requireAuth, getCurrencies);

router.get("/payment-types/crud", requireAuth, getPaymentTypes);
router.post("/payment-types", requireAuth, createPaymentType);
router.put("/payment-types/:id", requireAuth, updatePaymentType);
router.delete("/payment-types/:id", requireAuth, deletePaymentType);

router.get("/vessels/crud", requireAuth, getVessels);
router.post("/vessels", requireAuth, createVessel);
router.put("/vessels/:id", requireAuth, updateVessel);
router.delete("/vessels/:id", requireAuth, deleteVessel);

router.get("/categories/crud", requireAuth, getCategories);
router.post("/categories", requireAuth, createCategory);
router.put("/categories/:id", requireAuth, updateCategory);
router.delete("/categories/:id", requireAuth, deleteCategory);

router.get("/subcategories/crud", requireAuth, getSubcategories);
router.post("/subcategories", requireAuth, createSubcategory);
router.put("/subcategories/:id", requireAuth, updateSubcategory);
router.delete("/subcategories/:id", requireAuth, deleteSubcategory);

router.get("/places/crud", requireAuth, getPlaces);
router.post("/places", requireAuth, createPlace);
router.put("/places/:id", requireAuth, updatePlace);
router.delete("/places/:id", requireAuth, deletePlace);

router.get("/thirdParty/crud", requireAuth, getThirdParties);
router.post("/thirdParty", requireAuth, createThirdParty);
router.put("/thirdParty/:id", requireAuth, updateThirdParty);
router.delete("/thirdParty/:id", requireAuth, deleteThirdParty);

router.get("/banks/crud", requireAuth, getBanks);
router.post("/banks", requireAuth, createBank);
router.put("/banks/:id", requireAuth, updateBank);
router.delete("/banks/:id", requireAuth, deleteBank);

router.get("/allStatus", requireAuth, getAllStatus);
router.put("/updateStatus/:id", requireAuth, updateStatus);
router.put("/toggleSendEmail/:id", requireAuth, toggleSendEmail);
router.post("/addStatus", requireAuth, addNewStatus);
router.delete("/deleteStatus/:id", requireAuth, deleteStatus);

router.post(
  "/bug-report",
  requireAuth,
  bugReportUpload.array("attachments", 3),
  createBugReport,
);
router.get("/bug-report", requireAuth, getBugReports);
router.put(
  "/bug-report/:id",
  requireAuth,
  bugReportUpload.array("attachments", 3),
  updateBugReport,
);
router.delete("/bug-report/:id", requireAuth, deleteBugReport);

router.get("/dashboard", requireAuth, getDashboardData);
router.get("/modules", requireAuth, getModules);

router.post("/companies", requireAuth, companyUpload, createCompany);
router.get("/companies", requireAuth, getCompanies);
router.get("/companies/:id", requireAuth, getCompanies);
router.put("/companies/:id", requireAuth, companyUpload, updateCompany);

router.post("/documents", requireAuth, createDocumentTemplate);
router.get("/documents", requireAuth, getDocumentTemplates);
router.get("/documents/:id", requireAuth, getDocumentTemplates);
router.put("/documents/:id", requireAuth, updateDocumentTemplate);

export default router;
