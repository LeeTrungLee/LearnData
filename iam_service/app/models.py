from werkzeug.security import generate_password_hash, check_password_hash

USERS = []

class User:
	def __init__(self, username, password, role="user"):
		self.username = username
		self.password_hash = generate_password_hash(password)
		self.role = role
	
	def check_password(self, password):
		return check_password_hash(self.password_hash, password)

def get_user(username):
	for user in USERS:
		if user.username == username:
			return user
	return None

def add_user(username, password, role="user"):
	if get_user(username):
		return None
	user = User(username, password, role)
	USERS.append(user)
	return user
