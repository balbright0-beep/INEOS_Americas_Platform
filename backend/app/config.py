import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///platform.db")
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
ADMIN_DEFAULT_PASSWORD = os.environ.get("ADMIN_DEFAULT_PASSWORD", "admin123")
INTERNAL_PASSWORD = os.environ.get("INTERNAL_PASSWORD", "ineos2026")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "https://ineos-dashboard-app.onrender.com")
ALLOCATION_URL = os.environ.get("ALLOCATION_URL", "https://ineos-allocation-app.onrender.com")
INCENTIVE_URL = os.environ.get("INCENTIVE_URL", "https://ineos-incentive-app.onrender.com")
FLEET_URL = os.environ.get("FLEET_URL", "https://ineos-fleet-app.onrender.com")
