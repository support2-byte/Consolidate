/**
 * @swagger
 * components:
 *   schemas:
 *     Vendor:
 *       type: object
 *       properties:
 *         zoho_id: { type: string }
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *         address: { type: string }
 *         createdAt: { type: string, format: date-time }
 *         updatedAt: { type: string, format: date-time }
 *     CreateVendorRequest:
 *       type: object
 *       required: [name, email]
 *       properties:
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *         address: { type: string }
 *     UpdateVendorRequest:
 *       type: object
 *       properties:
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *         address: { type: string }
 *     VendorContact:
 *       type: object
 *       properties:
 *         contact_person_id: { type: string }
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *     SaveVendorContactsRequest:
 *       type: object
 *       properties:
 *         contacts:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/VendorContact'
 *     VendorDocument:
 *       type: object
 *       properties:
 *         document_id: { type: string }
 *         zoho_id: { type: string }
 *         fileName: { type: string }
 *         fileUrl: { type: string }
 *         uploadedAt: { type: string, format: date-time }
 *     UpdateVendorDocumentRequest:
 *       type: object
 *       properties:
 *         fileName: { type: string }
 */
