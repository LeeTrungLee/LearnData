from flask import Blueprint, request, jsonify
from app.models import add_user, get_user
from app.auth import generator_token, decode_token

bp = Blueprint('auth', __name__)

@bp.route('/register', methods=['POST'])
def register():
	data = request.get_json()
	if not data or 'username' not in data or 'password' not in data:
		return jsonify({'message': 'Nhập thiếu trường thông tin'}), 400
	
	user = add_user(data['username'], data['password'])
	if not user:
		return jsonify({'message': 'Người dùng đã tồn tại'}), 400
	
	return jsonify({'message': 'Người dùng đăng ký thành công'}), 201

@bp.route('/login', methods=['POST'])
def login():
	data = request.get_json()
	user = get_user(data.get('username'))
	if user and user.check_password(data.get('password')):
		token = generator_token(user)
		return jsonify({'token': token})
	return jsonify({'message': 'Thông tin đăng nhập không hợp lệ!'}), 401

@bp.route('/', methods=['GET'])
def home():
	return {
		'message': 'Hello. This page is home!',
		'routes': ['/api/register', 'api/login']
	}