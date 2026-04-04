import bcrypt
from app.models import User
from app.config import ADMIN_DEFAULT_PASSWORD, INTERNAL_PASSWORD


def hash_pw(pw):
    return bcrypt.hashpw(pw.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")


def seed_database(db):
    if db.query(User).count() == 0:
        db.add(User(username="admin", password_hash=hash_pw(ADMIN_DEFAULT_PASSWORD), role="admin"))
        db.add(User(username="internal", password_hash=hash_pw(INTERNAL_PASSWORD), role="internal"))
        db.commit()
        print("Seeded admin + internal users")
