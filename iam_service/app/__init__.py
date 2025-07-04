from flask import Flask
from app.routers import bp

def create_app():
	app = Flask(__name__)
	app.config['SECRET_KEY'] = 'super-secret-key'
	app.config['JWT_SECRET_KEY'] = 'jwt-secret-key'

	app.register_blueprint(bp, url_prefix='/api')

	return app
