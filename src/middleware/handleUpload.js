import { containerUpload } from "../middleware/upload.js";

export const handleContainerUpload = (req, res, next) => {
  containerUpload(req, res, (err) => {
    if (err) {
      if (err.code === "LIMIT_FILE_SIZE") {
        return res
          .status(400)
          .json({ error: "Each file must be 5MB or smaller" });
      }
      if (
        err.code === "LIMIT_FILE_COUNT" ||
        err.code === "LIMIT_UNEXPECTED_FILE"
      ) {
        return res
          .status(400)
          .json({ error: "You can upload a maximum of 5 files" });
      }
      return res
        .status(400)
        .json({ error: err.message || "File upload failed" });
    }
    next();
  });
};
