import pool from "../db/pool.js";

export async function getEffectivePermissions(userId, roleId) {
  const result = await pool.query(
    `SELECT m.code AS module_code, pa.code AS action_code
     FROM role_permissions rp
     JOIN modules m ON m.id = rp.module_id
     JOIN permission_actions pa ON pa.id = rp.action_id
     WHERE rp.role_id = $1

     UNION

     SELECT m.code AS module_code, pa.code AS action_code
     FROM user_permission_overrides upo
     JOIN modules m ON m.id = upo.module_id
     JOIN permission_actions pa ON pa.id = upo.action_id
     WHERE upo.user_id = $2 AND upo.granted = true

     EXCEPT

     SELECT m.code AS module_code, pa.code AS action_code
     FROM user_permission_overrides upo
     JOIN modules m ON m.id = upo.module_id
     JOIN permission_actions pa ON pa.id = upo.action_id
     WHERE upo.user_id = $2 AND upo.granted = false`,
    [roleId, userId],
  );

  return result.rows.map((r) => `${r.module_code}.${r.action_code}`);
}
