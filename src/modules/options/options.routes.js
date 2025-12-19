// src/modules/options/routes.js
import express from 'express';
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
  updateEtaConfig
} from './options.controllers.js';

const router = express.Router();

// Mount routes for dropdown options (without /crud for simple GET lists)
router.get('/shippers', getShippers);
router.get('/consignees', getConsignees);
router.get('/origins', getOrigins);
router.get('/destinations', getDestinations);
router.get('/banks', getBanks); // Updated: Removed /crud for dropdown compatibility
router.get('/payment-types', getPaymentTypeOptions); // For dropdown options
router.get('/vessels', getVessels); // Updated: Removed /crud for dropdown compatibility
router.get('/shipping-lines', getShippingLines);
router.get('/currencies', getCurrencies);
router.get('/statuses', getStatuses);
router.get('/container-statuses', getContainerStatuses);

// CRUD routes (keep /crud where full list with actions is needed)
router.get('/payment-types/crud', getPaymentTypes);
router.post('/payment-types', createPaymentType);
router.put('/payment-types/:id', updatePaymentType);
router.delete('/payment-types/:id', deletePaymentType);
router.get('/vessels/crud', getVessels); // Keep for full CRUD if needed, but primary is without /crud
router.post('/vessels', createVessel);
router.put('/vessels/:id', updateVessel);
router.delete('/vessels/:id', deleteVessel);
router.get('/categories/crud', getCategories);
router.post('/categories', createCategory);
router.put('/categories/:id', updateCategory);
router.delete('/categories/:id', deleteCategory);
router.get('/subcategories/crud', getSubcategories);
router.post('/subcategories', createSubcategory);
router.put('/subcategories/:id', updateSubcategory);
router.delete('/subcategories/:id', deleteSubcategory);
router.get('/places/crud', getPlaces);
router.post('/places', createPlace);
router.put('/places/:id', updatePlace);
router.delete('/places/:id', deletePlace);
router.get("/thirdParty/crud", getThirdParties);
router.post('/thirdParty', createThirdParty);
router.put('/thirdParty/:id', updateThirdParty);
router.delete('/thirdParty/:id', deleteThirdParty);
router.get('/banks/crud', getBanks); // Keep for full CRUD if needed, but primary is without /crud
router.post('/banks', createBank);
router.put('/banks/:id', updateBank);
router.delete('/banks/:id', deleteBank);
router.post('/eta-configs', createEtaConfig);
router.put('/eta-configs/:id', updateEtaConfig);
router.delete('/eta-configs/:id', deleteEtaConfig);
router.get('/eta-configs', getEtaConfigs);



export default router;