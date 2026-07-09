/**
 * @swagger
 * components:
 *   schemas:
 *     RegisterRequest:
 *       type: object
 *       required: [email, password, name]
 *       properties:
 *         email: { type: string, format: email }
 *         password: { type: string, format: password }
 *         name: { type: string }
 *     LoginRequest:
 *       type: object
 *       required: [email, password]
 *       properties:
 *         email: { type: string, format: email }
 *         password: { type: string, format: password }
 *     User:
 *       type: object
 *       properties:
 *         id: { type: string }
 *         email: { type: string, format: email }
 *         name: { type: string }
 *         role: { type: string }
 *         createdAt: { type: string, format: date-time }
 *     CreateUserRequest:
 *       type: object
 *       required: [email, password, name, role]
 *       properties:
 *         email: { type: string, format: email }
 *         password: { type: string, format: password }
 *         name: { type: string }
 *         role: { type: string }
 *     UpdateUserRequest:
 *       type: object
 *       properties:
 *         email: { type: string, format: email }
 *         name: { type: string }
 *         role: { type: string }
 *     RolePermissions:
 *       type: object
 *       properties:
 *         roleName: { type: string }
 *         permissions:
 *           type: array
 *           items: { type: string }
 *     ErrorResponse:
 *       type: object
 *       properties:
 *         message: { type: string }
 */
