import geoip from "geoip-lite";

export function getLocationFromIp(ip) {
  const geo = geoip.lookup(ip);
  if (!geo) return null;
  return [geo.city, geo.region, geo.country].filter(Boolean).join(", ");
}
