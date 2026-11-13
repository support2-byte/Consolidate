const ACCESS = {
  receiver: ["storage", "delivery", "gatepass"],
  sender: ["drop-off"],
  both: ["drop-off", "storage", "delivery", "gatepass"],
};

export const requireFeature = (feature) => (req, res, next) => {
  if (!ACCESS[req.user?.type]?.includes(feature)) {
    return res.status(403).json({
      success: false,
      message: "You don't have access to this service.",
    });
  }
  next();
};
