"""Logging configuration using loguru."""

import sys
from pathlib import Path
from loguru import logger
from fraud_detection.config.settings import settings

def setup_logger() -> logger:
    """Configure and return a logger instance."""
    
    logger.remove()
    
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        level=settings.LOG_LEVEL,
        colorize=True,
        backtrace=True,
        diagnose=True
    )
    
    logger.add(
        settings.LOGS_DIR / "app.log",
        format="{time} | {level} | {name}:{function}:{line} | {message}",
        level="DEBUG",
        rotation="1 day",
        retention="30 days",
        compression="gz",
        backtrace=True,
        diagnose=True
    )
    
    logger.add(
        settings.LOGS_DIR / "error.log",
        format="{time} | {level} | {name}:{function}:{line} | {message}",
        level="ERROR",
        rotation="1 week",
        retention="90 days",
        compression="gz"
    )
    
    if settings.ENABLE_AUDIT_LOGGING:
        logger.add(
            settings.LOGS_DIR / "audit.log",
            format="{time} | {message}",
            level="INFO",
            rotation="1 day",
            retention="365 days",
            compression="gz",
            filter=lambda record: "audit" in record["extra"]
        )
    
    logger.add(
        settings.LOGS_DIR / "fraud_audit.log",
        format="{time} | {message}",
        level="INFO",
        rotation="1 day",
        retention="365 days",
        compression="gz",
        filter=lambda record: "fraud_audit" in record["extra"]
    )
    
    logger.add(
        settings.LOGS_DIR / "gdpr_deletions.log",
        format="{time} | {message}",
        level="INFO",
        rotation="1 month",
        retention="2555 days",
        compression="gz",
        filter=lambda record: "deletion" in record["extra"]
    )
    
    logger.add(
        settings.LOGS_DIR / "verification.log",
        format="{time} | {message}",
        level="INFO",
        rotation="1 week",
        retention="90 days",
        compression="gz",
        filter=lambda record: "verify" in record["extra"]
    )
    
    return logger

logger = setup_logger()