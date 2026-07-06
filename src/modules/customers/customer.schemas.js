/**
 * @swagger
 * components:
 *   schemas:
 *     Customer:
 *       type: object
 *       properties:
 *         zoho_id: { type: string }
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *         address: { type: string }
 *         createdAt: { type: string, format: date-time }
 *         updatedAt: { type: string, format: date-time }
 *     CreateCustomerRequest:
 *       type: object
 *       required: [name, email]
 *       properties:
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *         address: { type: string }
 *     UpdateCustomerRequest:
 *       type: object
 *       properties:
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *         address: { type: string }
 *     Contact:
 *       type: object
 *       properties:
 *         contact_person_id: { type: string }
 *         name: { type: string }
 *         email: { type: string, format: email }
 *         phone: { type: string }
 *     SaveContactsRequest:
 *       type: object
 *       properties:
 *         contacts:
 *           type: array
 *           items:
 *             $ref: '#/components/schemas/Contact'
 *     CustomerDocument:
 *       type: object
 *       properties:
 *         document_id: { type: string }
 *         zoho_id: { type: string }
 *         fileName: { type: string }
 *         fileUrl: { type: string }
 *         uploadedAt: { type: string, format: date-time }
 *     UpdateDocumentRequest:
 *       type: object
 *       properties:
 *         fileName: { type: string }
 */
