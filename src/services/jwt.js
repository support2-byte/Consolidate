export function signAccessToken(payload) {
  return jwt.sign(payload, JWT_SECRET, {
    expiresIn: "15m",
    algorithm: "HS256",
  });
}

export function signRefreshToken(payload) {
  return jwt.sign(payload, JWT_REFRESH_SECRET, {
    expiresIn: "7d",
    algorithm: "HS256",
  });
}
