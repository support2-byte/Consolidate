/**
 * @swagger
 * components:
 *   schemas:
 *     NamedOption:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *     NamedOptionRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *     Vessel:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *         imo: { type: string }
 *     VesselRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *         imo: { type: string }
 *     PaymentType:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *     PaymentTypeRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *     Category:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *     CategoryRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *     Subcategory:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *         categoryId: { type: string }
 *     SubcategoryRequest:
 *       type: object
 *       required: [name, categoryId]
 *       properties:
 *         name: { type: string }
 *         categoryId: { type: string }
 *     Place:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *     PlaceRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *     ThirdParty:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *     ThirdPartyRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *     Bank:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *     BankRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *     EtaConfig:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *         days: { type: integer }
 *     EtaConfigRequest:
 *       type: object
 *       required: [name, days]
 *       properties:
 *         name: { type: string }
 *         days: { type: integer }
 *     StatusItem:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         name: { type: string }
 *         order: { type: integer }
 *     StatusRequest:
 *       type: object
 *       required: [name]
 *       properties:
 *         name: { type: string }
 *         order: { type: integer }
 *     BugReport:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         title: { type: string }
 *         description: { type: string }
 *         attachments:
 *           type: array
 *           items: { type: string }
 *         createdAt: { type: string, format: date-time }
 *     BugReportRequest:
 *       type: object
 *       required: [title, description]
 *       properties:
 *         title: { type: string }
 *         description: { type: string }
 *         attachments:
 *           type: array
 *           items: { type: string, format: binary }
 */
