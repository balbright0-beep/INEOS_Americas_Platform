from sqlalchemy import Column, Integer, String, Text, DateTime, func
from app.database import Base


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False)  # admin, internal, dealer
    dealer_name = Column(String)  # only for dealer accounts
    created_at = Column(DateTime, server_default=func.now())


class Bulletin(Base):
    __tablename__ = "bulletins"
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    priority = Column(String, default="info")  # info, important, urgent
    audience = Column(String, default="both")  # internal, dealer, both
    created_by = Column(String)
    created_at = Column(DateTime, server_default=func.now())


class LinkCategory(Base):
    __tablename__ = "link_categories"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    sort_order = Column(Integer, default=0)


class Link(Base):
    __tablename__ = "links"
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    description = Column(String)
    sort_order = Column(Integer, default=0)


class AppState(Base):
    __tablename__ = "app_state"
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False)
    value = Column(Text)


# ===== DEALER DATA TABLES =====

class Vehicle(Base):
    __tablename__ = "vehicles"
    id = Column(Integer, primary_key=True)
    vin = Column(String, index=True)
    dealer = Column(String, index=True)
    market = Column(String)
    country = Column(String)
    body = Column(String)
    model_year = Column(String)
    status = Column(String, index=True)
    msrp = Column(Integer)
    trim = Column(String)
    ext_color = Column(String)
    int_color = Column(String)
    roof = Column(String)
    wheels = Column(String)
    channel = Column(String)
    plant = Column(String)
    handover_date = Column(String)
    eta = Column(String)
    vessel = Column(String)
    days_on_lot = Column(Integer)
    so_number = Column(String)


class RetailSale(Base):
    __tablename__ = "retail_sales"
    id = Column(Integer, primary_key=True)
    dealer = Column(String, index=True)
    market = Column(String)
    vin = Column(String)
    vin_full = Column(String)
    body = Column(String)
    model_year = Column(String)
    trim = Column(String)
    ext_color = Column(String)
    int_color = Column(String)
    wheels = Column(String)
    channel = Column(String)
    msrp = Column(Integer)
    days_to_sell = Column(Integer)
    cvp = Column(String)
    handover_date = Column(String)


class DealerPerformance(Base):
    __tablename__ = "dealer_performance"
    id = Column(Integer, primary_key=True)
    dealer = Column(String, index=True)
    market = Column(String)
    handovers = Column(Integer, default=0)
    cvp = Column(Integer, default=0)
    wholesales = Column(Integer, default=0)
    on_ground = Column(Integer, default=0)
    dealer_stock = Column(Integer, default=0)
    leads = Column(Integer, default=0)
    test_drives = Column(Integer, default=0)
    td_completed = Column(Integer, default=0)
    td_show_pct = Column(String)
    lead_to_td_pct = Column(String)
    won = Column(Integer, default=0)
    lost = Column(Integer, default=0)
    mb30 = Column(String)
    mb60 = Column(String)
    mb90 = Column(String)


class RegionalSales(Base):
    __tablename__ = "regional_sales"
    id = Column(Integer, primary_key=True)
    region = Column(String)
    sw = Column(Integer, default=0)
    qm = Column(Integer, default=0)
    svo = Column(Integer, default=0)
    total = Column(Integer, default=0)
    objective = Column(Integer, default=0)
    pct_objective = Column(String)
    cvp = Column(Integer, default=0)


# ===== AUDIT & HISTORY =====

class AuditLog(Base):
    __tablename__ = "audit_log"
    id = Column(Integer, primary_key=True)
    action = Column(String, nullable=False)  # login, upload, create_user, delete_user, create_bulletin, etc.
    user = Column(String)
    detail = Column(Text)
    created_at = Column(DateTime, server_default=func.now())


class UploadHistory(Base):
    __tablename__ = "upload_history"
    id = Column(Integer, primary_key=True)
    filename = Column(String)
    uploaded_by = Column(String)
    vehicles_count = Column(Integer, default=0)
    retail_sales_count = Column(Integer, default=0)
    performance_count = Column(Integer, default=0)
    status = Column(String, default="success")
    created_at = Column(DateTime, server_default=func.now())


class MonthlySnapshot(Base):
    __tablename__ = "monthly_snapshots"
    id = Column(Integer, primary_key=True)
    month = Column(String, index=True)  # YYYY-MM format
    dealer = Column(String, index=True)
    market = Column(String)
    sales = Column(Integer, default=0)
    handovers = Column(Integer, default=0)
    on_ground = Column(Integer, default=0)
    leads = Column(Integer, default=0)
    test_drives = Column(Integer, default=0)
    won = Column(Integer, default=0)
    avg_days_to_sell = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now())
