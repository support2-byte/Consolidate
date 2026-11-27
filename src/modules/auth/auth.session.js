import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET;

export const IDLE_TIMEOUT_MINUTES = 30;
export const ACCESS_TOKEN_TTL = `${IDLE_TIMEOUT_MINUTES}m`;
export const ACCESS_TOKEN_MAX_AGE_MS = IDLE_TIMEOUT_MINUTES * 60 * 1000;

const COOKIE_OPTIONS = {
  httpOnly: true,
  secure: true,
  sameSite: "none",
};

export function signAccessToken({ id, email, roleId, roleName }) {
  return jwt.sign({ id, email, roleId, roleName }, JWT_SECRET, {
    algorithm: "HS256",
    expiresIn: ACCESS_TOKEN_TTL,
  });
}

export function setAccessCookie(res, token) {
  res.cookie("accessToken", token, {
    ...COOKIE_OPTIONS,
    maxAge: ACCESS_TOKEN_MAX_AGE_MS,
  });
}

export function clearAuthCookies(res) {
  res.clearCookie("accessToken", COOKIE_OPTIONS);
  res.clearCookie("refreshToken", COOKIE_OPTIONS);
}
