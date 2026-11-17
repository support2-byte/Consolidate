const MAX_PARAM_LENGTH = 40;

export const validateRequestParams = (req, res, next) => {
  try {
    const pathname = req.path;

    const segments = pathname.split("/").filter(Boolean);

    for (const segment of segments) {
      let decodedSegment;

      try {
        decodedSegment = decodeURIComponent(segment);
      } catch {
        return res.status(400).json({
          success: false,
          message: "Invalid URL encoding.",
        });
      }

      if (Array.from(decodedSegment).length > MAX_PARAM_LENGTH) {
        return res.status(400).json({
          success: false,
          message: `URL parameter cannot exceed ${MAX_PARAM_LENGTH} characters.`,
        });
      }
    }

    next();
  } catch (error) {
    next(error);
  }
};
