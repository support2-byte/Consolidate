import winston from "winston";
import chalk from "chalk";

const logLevels = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
};

const logger = winston.createLogger({
  levels: logLevels,
  level: process.env.LOG_LEVEL || "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json(),
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.colorize({ level: true }),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          const metaString =
            Object.keys(meta).length > 0
              ? chalk.hex("#8B5CF6")(
                  JSON.stringify(meta, null, 2).replace(/\r?\n\s*/g, " "),
                )
              : "";

          return `${chalk.dim(timestamp)} [${level}] ${chalk.cyan(message)}${
            metaString ? ` ${metaString}` : ""
          }`;
        }),
      ),
    }),
  ],
});

export default logger;
