// src/modules/options/routes.js
import express from "express";
import {
  getShippers,
  getConsignees,
  getOrigins,
  getDestinations,
  getBanks,
  getPaymentTypes,
  getVessels,
  createVessel,
  updateVessel,
  deleteVessel,
  getShippingLines,
  getCurrencies,
  getStatuses,
  getContainerStatuses,
  createPaymentType,
  updatePaymentType,
  deletePaymentType,
  getPaymentTypeOptions,
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
  createEtaConfig,
  deleteEtaConfig,
  getEtaConfigs,
  updateEtaConfig,
  getAllStatus,
  updateStatus,
  addNewStatus,
  deleteStatus,
  createBugReport,
  getBugReports,
  updateBugReport,
  deleteBugReport,
} from "./options.controllers.js";
import { bugReportUpload } from "../../middleware/upload.js";

const router = express.Router();

// Mount routes for dropdown options (without /crud for simple GET lists)

/**
 * @swagger
 * /api/options/shippers:
 *   get:
 *     summary: List shipper options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of shippers
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/NamedOption'
 */
router.get("/shippers", getShippers);

/**
 * @swagger
 * /api/options/consignees:
 *   get:
 *     summary: List consignee options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of consignees
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/NamedOption'
 */
router.get("/consignees", getConsignees);

/**
 * @swagger
 * /api/options/origins:
 *   get:
 *     summary: List origin options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of origins
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/NamedOption'
 */
router.get("/origins", getOrigins);

/**
 * @swagger
 * /api/options/destinations:
 *   get:
 *     summary: List destination options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of destinations
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/NamedOption'
 */
router.get("/destinations", getDestinations);

/**
 * @swagger
 * /api/options/banks:
 *   get:
 *     summary: List bank options (dropdown)
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of banks
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Bank'
 */
router.get("/banks", getBanks); // Updated: Removed /crud for dropdown compatibility

/**
 * @swagger
 * /api/options/payment-types:
 *   get:
 *     summary: List payment type options (dropdown)
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of payment types
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/PaymentType'
 */
router.get("/payment-types", getPaymentTypeOptions); // For dropdown options

/**
 * @swagger
 * /api/options/vessels:
 *   get:
 *     summary: List vessel options (dropdown)
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of vessels
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Vessel'
 */
router.get("/vessels", getVessels); // Updated: Removed /crud for dropdown compatibility

/**
 * @swagger
 * /api/options/shipping-lines:
 *   get:
 *     summary: List shipping line options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of shipping lines
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/NamedOption'
 */
router.get("/shipping-lines", getShippingLines);

/**
 * @swagger
 * /api/options/currencies:
 *   get:
 *     summary: List currency options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of currencies
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/NamedOption'
 */
router.get("/currencies", getCurrencies);

/**
 * @swagger
 * /api/options/statuses:
 *   get:
 *     summary: List general status options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of statuses
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/StatusItem'
 */
router.get("/statuses", getStatuses);

/**
 * @swagger
 * /api/options/container-statuses:
 *   get:
 *     summary: List container status options
 *     tags: [Options - Dropdowns]
 *     responses:
 *       200:
 *         description: List of container statuses
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/StatusItem'
 */
router.get("/container-statuses", getContainerStatuses);

// CRUD routes (keep /crud where full list with actions is needed)

/**
 * @swagger
 * /api/options/payment-types/crud:
 *   get:
 *     summary: List payment types (full CRUD list)
 *     tags: [Options - Payment Types]
 *     responses:
 *       200:
 *         description: List of payment types
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/PaymentType'
 */
router.get("/payment-types/crud", getPaymentTypes);

/**
 * @swagger
 * /api/options/payment-types:
 *   post:
 *     summary: Create a payment type
 *     tags: [Options - Payment Types]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/PaymentTypeRequest'
 *     responses:
 *       201:
 *         description: Payment type created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/PaymentType'
 */
router.post("/payment-types", createPaymentType);

/**
 * @swagger
 * /api/options/payment-types/{id}:
 *   put:
 *     summary: Update a payment type
 *     tags: [Options - Payment Types]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/PaymentTypeRequest'
 *     responses:
 *       200:
 *         description: Payment type updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/PaymentType'
 *       404:
 *         description: Payment type not found
 */
router.put("/payment-types/:id", updatePaymentType);

/**
 * @swagger
 * /api/options/payment-types/{id}:
 *   delete:
 *     summary: Delete a payment type
 *     tags: [Options - Payment Types]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Payment type deleted
 *       404:
 *         description: Payment type not found
 */
router.delete("/payment-types/:id", deletePaymentType);

/**
 * @swagger
 * /api/options/vessels/crud:
 *   get:
 *     summary: List vessels (full CRUD list)
 *     tags: [Options - Vessels]
 *     responses:
 *       200:
 *         description: List of vessels
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Vessel'
 */
router.get("/vessels/crud", getVessels); // Keep for full CRUD if needed, but primary is without /crud

/**
 * @swagger
 * /api/options/vessels:
 *   post:
 *     summary: Create a vessel
 *     tags: [Options - Vessels]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/VesselRequest'
 *     responses:
 *       201:
 *         description: Vessel created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Vessel'
 */
router.post("/vessels", createVessel);

/**
 * @swagger
 * /api/options/vessels/{id}:
 *   put:
 *     summary: Update a vessel
 *     tags: [Options - Vessels]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/VesselRequest'
 *     responses:
 *       200:
 *         description: Vessel updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Vessel'
 *       404:
 *         description: Vessel not found
 */
router.put("/vessels/:id", updateVessel);

/**
 * @swagger
 * /api/options/vessels/{id}:
 *   delete:
 *     summary: Delete a vessel
 *     tags: [Options - Vessels]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Vessel deleted
 *       404:
 *         description: Vessel not found
 */
router.delete("/vessels/:id", deleteVessel);

/**
 * @swagger
 * /api/options/categories/crud:
 *   get:
 *     summary: List categories
 *     tags: [Options - Categories]
 *     responses:
 *       200:
 *         description: List of categories
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Category'
 */
router.get("/categories/crud", getCategories);

/**
 * @swagger
 * /api/options/categories:
 *   post:
 *     summary: Create a category
 *     tags: [Options - Categories]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/CategoryRequest'
 *     responses:
 *       201:
 *         description: Category created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Category'
 */
router.post("/categories", createCategory);

/**
 * @swagger
 * /api/options/categories/{id}:
 *   put:
 *     summary: Update a category
 *     tags: [Options - Categories]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/CategoryRequest'
 *     responses:
 *       200:
 *         description: Category updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Category'
 *       404:
 *         description: Category not found
 */
router.put("/categories/:id", updateCategory);

/**
 * @swagger
 * /api/options/categories/{id}:
 *   delete:
 *     summary: Delete a category
 *     tags: [Options - Categories]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Category deleted
 *       404:
 *         description: Category not found
 */
router.delete("/categories/:id", deleteCategory);

/**
 * @swagger
 * /api/options/subcategories/crud:
 *   get:
 *     summary: List subcategories
 *     tags: [Options - Subcategories]
 *     responses:
 *       200:
 *         description: List of subcategories
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Subcategory'
 */
router.get("/subcategories/crud", getSubcategories);

/**
 * @swagger
 * /api/options/subcategories:
 *   post:
 *     summary: Create a subcategory
 *     tags: [Options - Subcategories]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/SubcategoryRequest'
 *     responses:
 *       201:
 *         description: Subcategory created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Subcategory'
 */
router.post("/subcategories", createSubcategory);

/**
 * @swagger
 * /api/options/subcategories/{id}:
 *   put:
 *     summary: Update a subcategory
 *     tags: [Options - Subcategories]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/SubcategoryRequest'
 *     responses:
 *       200:
 *         description: Subcategory updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Subcategory'
 *       404:
 *         description: Subcategory not found
 */
router.put("/subcategories/:id", updateSubcategory);

/**
 * @swagger
 * /api/options/subcategories/{id}:
 *   delete:
 *     summary: Delete a subcategory
 *     tags: [Options - Subcategories]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Subcategory deleted
 *       404:
 *         description: Subcategory not found
 */
router.delete("/subcategories/:id", deleteSubcategory);

/**
 * @swagger
 * /api/options/places/crud:
 *   get:
 *     summary: List places
 *     tags: [Options - Places]
 *     responses:
 *       200:
 *         description: List of places
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Place'
 */
router.get("/places/crud", getPlaces);

/**
 * @swagger
 * /api/options/places:
 *   post:
 *     summary: Create a place
 *     tags: [Options - Places]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/PlaceRequest'
 *     responses:
 *       201:
 *         description: Place created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Place'
 */
router.post("/places", createPlace);

/**
 * @swagger
 * /api/options/places/{id}:
 *   put:
 *     summary: Update a place
 *     tags: [Options - Places]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/PlaceRequest'
 *     responses:
 *       200:
 *         description: Place updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Place'
 *       404:
 *         description: Place not found
 */
router.put("/places/:id", updatePlace);

/**
 * @swagger
 * /api/options/places/{id}:
 *   delete:
 *     summary: Delete a place
 *     tags: [Options - Places]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Place deleted
 *       404:
 *         description: Place not found
 */
router.delete("/places/:id", deletePlace);

/**
 * @swagger
 * /api/options/thirdParty/crud:
 *   get:
 *     summary: List third parties
 *     tags: [Options - Third Parties]
 *     responses:
 *       200:
 *         description: List of third parties
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/ThirdParty'
 */
router.get("/thirdParty/crud", getThirdParties);

/**
 * @swagger
 * /api/options/thirdParty:
 *   post:
 *     summary: Create a third party
 *     tags: [Options - Third Parties]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/ThirdPartyRequest'
 *     responses:
 *       201:
 *         description: Third party created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ThirdParty'
 */
router.post("/thirdParty", createThirdParty);

/**
 * @swagger
 * /api/options/thirdParty/{id}:
 *   put:
 *     summary: Update a third party
 *     tags: [Options - Third Parties]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/ThirdPartyRequest'
 *     responses:
 *       200:
 *         description: Third party updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/ThirdParty'
 *       404:
 *         description: Third party not found
 */
router.put("/thirdParty/:id", updateThirdParty);

/**
 * @swagger
 * /api/options/thirdParty/{id}:
 *   delete:
 *     summary: Delete a third party
 *     tags: [Options - Third Parties]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Third party deleted
 *       404:
 *         description: Third party not found
 */
router.delete("/thirdParty/:id", deleteThirdParty);

/**
 * @swagger
 * /api/options/banks/crud:
 *   get:
 *     summary: List banks (full CRUD list)
 *     tags: [Options - Banks]
 *     responses:
 *       200:
 *         description: List of banks
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Bank'
 */
router.get("/banks/crud", getBanks); // Keep for full CRUD if needed, but primary is without /crud

/**
 * @swagger
 * /api/options/banks:
 *   post:
 *     summary: Create a bank
 *     tags: [Options - Banks]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/BankRequest'
 *     responses:
 *       201:
 *         description: Bank created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Bank'
 */
router.post("/banks", createBank);

/**
 * @swagger
 * /api/options/banks/{id}:
 *   put:
 *     summary: Update a bank
 *     tags: [Options - Banks]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/BankRequest'
 *     responses:
 *       200:
 *         description: Bank updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Bank'
 *       404:
 *         description: Bank not found
 */
router.put("/banks/:id", updateBank);

/**
 * @swagger
 * /api/options/banks/{id}:
 *   delete:
 *     summary: Delete a bank
 *     tags: [Options - Banks]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Bank deleted
 *       404:
 *         description: Bank not found
 */
router.delete("/banks/:id", deleteBank);

/**
 * @swagger
 * /api/options/eta-configs:
 *   post:
 *     summary: Create an ETA config
 *     tags: [Options - ETA Configs]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/EtaConfigRequest'
 *     responses:
 *       201:
 *         description: ETA config created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/EtaConfig'
 */
router.post("/eta-configs", createEtaConfig);

/**
 * @swagger
 * /api/options/eta-configs/{id}:
 *   put:
 *     summary: Update an ETA config
 *     tags: [Options - ETA Configs]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/EtaConfigRequest'
 *     responses:
 *       200:
 *         description: ETA config updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/EtaConfig'
 *       404:
 *         description: ETA config not found
 */
router.put("/eta-configs/:id", updateEtaConfig);

/**
 * @swagger
 * /api/options/eta-configs/{id}:
 *   delete:
 *     summary: Delete an ETA config
 *     tags: [Options - ETA Configs]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: ETA config deleted
 *       404:
 *         description: ETA config not found
 */
router.delete("/eta-configs/:id", deleteEtaConfig);

/**
 * @swagger
 * /api/options/eta-configs:
 *   get:
 *     summary: List ETA configs
 *     tags: [Options - ETA Configs]
 *     responses:
 *       200:
 *         description: List of ETA configs
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/EtaConfig'
 */
router.get("/eta-configs", getEtaConfigs);

/**
 * @swagger
 * /api/options/allStatus:
 *   get:
 *     summary: List all statuses
 *     tags: [Options - Status]
 *     responses:
 *       200:
 *         description: List of statuses
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/StatusItem'
 */
router.get("/allStatus", getAllStatus);

/**
 * @swagger
 * /api/options/updateStatus/{id}:
 *   put:
 *     summary: Update a status
 *     tags: [Options - Status]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/StatusRequest'
 *     responses:
 *       200:
 *         description: Status updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/StatusItem'
 *       404:
 *         description: Status not found
 */
router.put("/updateStatus/:id", updateStatus);

/**
 * @swagger
 * /api/options/addStatus:
 *   post:
 *     summary: Add a new status
 *     tags: [Options - Status]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             $ref: '#/components/schemas/StatusRequest'
 *     responses:
 *       201:
 *         description: Status created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/StatusItem'
 */
router.post("/addStatus", addNewStatus);

/**
 * @swagger
 * /api/options/deleteStatus/{id}:
 *   delete:
 *     summary: Delete a status
 *     tags: [Options - Status]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Status deleted
 *       404:
 *         description: Status not found
 */
router.delete("/deleteStatus/:id", deleteStatus);

/**
 * @swagger
 * /api/options/bug-report:
 *   post:
 *     summary: Create a bug report (with up to 3 attachments)
 *     tags: [Options - Bug Reports]
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/BugReportRequest'
 *     responses:
 *       201:
 *         description: Bug report created
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/BugReport'
 */
router.post(
  "/bug-report",
  bugReportUpload.array("attachments", 3),
  createBugReport,
);

/**
 * @swagger
 * /api/options/bug-report:
 *   get:
 *     summary: List bug reports
 *     tags: [Options - Bug Reports]
 *     responses:
 *       200:
 *         description: List of bug reports
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/BugReport'
 */
router.get("/bug-report", getBugReports);

/**
 * @swagger
 * /api/options/bug-report/{id}:
 *   put:
 *     summary: Update a bug report (with up to 3 attachments)
 *     tags: [Options - Bug Reports]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     requestBody:
 *       required: true
 *       content:
 *         multipart/form-data:
 *           schema:
 *             $ref: '#/components/schemas/BugReportRequest'
 *     responses:
 *       200:
 *         description: Bug report updated
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/BugReport'
 *       404:
 *         description: Bug report not found
 */
router.put(
  "/bug-report/:id",
  bugReportUpload.array("attachments", 3),
  updateBugReport,
);

/**
 * @swagger
 * /api/options/bug-report/{id}:
 *   delete:
 *     summary: Delete a bug report
 *     tags: [Options - Bug Reports]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Bug report deleted
 *       404:
 *         description: Bug report not found
 */
router.delete("/bug-report/:id", deleteBugReport);

export default router;
