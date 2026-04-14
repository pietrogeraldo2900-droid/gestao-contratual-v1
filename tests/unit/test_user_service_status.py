import pytest

from app.services import user_service as user_service_module
from app.services.user_service import UserAuthError, UserService


class FakeRepo:
    def __init__(self, user):
        self.user = dict(user)
        self.created = None

    def create_user(self, email, password_hash, *, status, role=None):
        self.created = {
            "id": 1,
            "email": email,
            "password": password_hash,
            "role": role,
            "status": status,
            "created_at": None,
            "approved_at": None,
            "last_login_at": None,
        }
        return dict(self.created)

    def get_user_by_email(self, email):
        if self.user and self.user.get("email") == email:
            return dict(self.user)
        return None

    def get_user_by_id(self, user_id):
        if self.user and int(self.user.get("id", 0) or 0) == int(user_id):
            return dict(self.user)
        return None

    def update_last_login(self, user_id):
        self.user["last_login_at"] = "now"


def _make_user(email, password_hash, status):
    return {
        "id": 7,
        "email": email,
        "password": password_hash,
        "status": status,
        "role": "operador",
        "created_at": None,
        "approved_at": None,
        "last_login_at": None,
    }


def test_register_user_defaults_pending():
    if user_service_module.bcrypt is None:
        pytest.skip("bcrypt not available")
    repo = FakeRepo({})
    service = UserService(repo)
    user = service.register_user("user@example.com", "SenhaSegura1")
    assert user["status"] == "pending"


def test_pending_user_cannot_login():
    if user_service_module.bcrypt is None:
        pytest.skip("bcrypt not available")
    password = "SenhaSegura1"
    hashed = user_service_module.bcrypt.hashpw(password.encode("utf-8"), user_service_module.bcrypt.gensalt()).decode(
        "utf-8"
    )
    repo = FakeRepo(_make_user("user@example.com", hashed, "pending"))
    service = UserService(repo)
    with pytest.raises(UserAuthError):
        service.authenticate_user("user@example.com", password)


def test_active_user_can_login():
    if user_service_module.bcrypt is None:
        pytest.skip("bcrypt not available")
    password = "SenhaSegura1"
    hashed = user_service_module.bcrypt.hashpw(password.encode("utf-8"), user_service_module.bcrypt.gensalt()).decode(
        "utf-8"
    )
    repo = FakeRepo(_make_user("user@example.com", hashed, "active"))
    service = UserService(repo)
    user = service.authenticate_user("user@example.com", password)
    assert user["email"] == "user@example.com"


@pytest.mark.parametrize("status", ["rejected", "disabled"])
def test_non_active_users_cannot_login(status):
    if user_service_module.bcrypt is None:
        pytest.skip("bcrypt not available")
    password = "SenhaSegura1"
    hashed = user_service_module.bcrypt.hashpw(password.encode("utf-8"), user_service_module.bcrypt.gensalt()).decode(
        "utf-8"
    )
    repo = FakeRepo(_make_user("user@example.com", hashed, status))
    service = UserService(repo)
    with pytest.raises(UserAuthError):
        service.authenticate_user("user@example.com", password)
