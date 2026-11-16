const MAX_PARAM_LENGTH = 20;

export const validateRequestParams = (req, res, next) => {
  try {
    // Remove query string and split URL into path segments
    const pathname = req.path;

    const segments = pathname.split("/").filter(Boolean);

    for (const segment of segments) {
      // Decode URL-encoded parameter values
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
