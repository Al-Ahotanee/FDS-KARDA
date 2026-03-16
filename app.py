"""
ARDA Fertilizer Distribution System - Production Ready
Flask Backend + Blockchain + Static Page Serving
Optimized for Render.com deployment
"""

from flask import Flask, request, jsonify, send_from_directory, send_file, Response
import sqlite3
import hashlib
import json
import os
from datetime import datetime
import qrcode
import io
import base64
from functools import wraps
import bleach
import logging

# ============= APP CONFIGURATION =============

app = Flask(__name__)

# Secret key from environment variable (never hardcode in production)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'change-this-in-production')

# Use /tmp for writable storage on Render (ephemeral, resets on redeploy)
# For persistent data, migrate to PostgreSQL (see README)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('DATA_DIR', BASE_DIR)

DATABASE = os.path.join(DATA_DIR, 'fertilizer.db')
BLOCKCHAIN_FILE = os.path.join(DATA_DIR, 'blockchain.json')
INVENTORY_BLOCKCHAIN_FILE = os.path.join(DATA_DIR, 'inventory_blockchain.json')

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


# ============= DATABASE HELPER FUNCTIONS =============

def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # Better concurrency
    conn.execute("PRAGMA foreign_keys=ON")     # Enforce FK constraints
    return conn


def init_db():
    """Initialize database with all tables"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS farmers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            password TEXT NOT NULL,
            phone TEXT,
            lga TEXT,
            ward TEXT,
            polling_unit TEXT,
            farm_size REAL,
            total_bags_received INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS admins (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            password TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS store_officers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            password TEXT NOT NULL,
            location TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            fertilizer_type TEXT NOT NULL,
            total_bags INTEGER NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            status TEXT DEFAULT 'pending',
            created_by TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS farmer_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            farmer_id TEXT NOT NULL,
            session_id INTEGER NOT NULL,
            requested_bags INTEGER NOT NULL,
            allocated_bags INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            qr_code TEXT,
            blockchain_hash TEXT,
            distributed_by TEXT,
            distributed_at TIMESTAMP,
            acknowledged BOOLEAN DEFAULT 0,
            acknowledged_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (farmer_id) REFERENCES farmers(id),
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fertilizer_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit TEXT DEFAULT 'bags',
            location TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lgas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            lga_id INTEGER NOT NULL,
            FOREIGN KEY (lga_id) REFERENCES lgas(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS polling_units (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            ward_id INTEGER NOT NULL,
            FOREIGN KEY (ward_id) REFERENCES wards(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            action TEXT NOT NULL,
            details TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully.")


# ============= BLOCKCHAIN FUNCTIONS =============

def init_blockchain():
    """Initialize blockchain files with genesis blocks"""
    for filepath, label in [(BLOCKCHAIN_FILE, 'distribution'), (INVENTORY_BLOCKCHAIN_FILE, 'inventory')]:
        if not os.path.exists(filepath):
            genesis = {
                'index': 0,
                'timestamp': datetime.now().isoformat(),
                'transactions': [],
                'previous_hash': '0',
                'hash': calculate_hash(0, datetime.now().isoformat(), [], '0')
            }
            with open(filepath, 'w') as f:
                json.dump([genesis], f, indent=2)
            logger.info(f"{label.capitalize()} blockchain initialized with genesis block.")


def calculate_hash(index, timestamp, transactions, previous_hash):
    value = str(index) + str(timestamp) + json.dumps(transactions, sort_keys=True) + str(previous_hash)
    return hashlib.sha256(value.encode()).hexdigest()


def _load_chain(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception:
        return []


def _save_chain(filepath, chain):
    with open(filepath, 'w') as f:
        json.dump(chain, f, indent=2)


def load_blockchain():
    return _load_chain(BLOCKCHAIN_FILE)


def save_blockchain(chain):
    _save_chain(BLOCKCHAIN_FILE, chain)


def load_inventory_blockchain():
    return _load_chain(INVENTORY_BLOCKCHAIN_FILE)


def save_inventory_blockchain(chain):
    _save_chain(INVENTORY_BLOCKCHAIN_FILE, chain)


def _append_block(load_fn, save_fn, transaction):
    chain = load_fn()
    if not chain:
        return None
    prev = chain[-1]
    block = {
        'index': prev['index'] + 1,
        'timestamp': datetime.now().isoformat(),
        'transactions': [transaction],
        'previous_hash': prev['hash'],
        'hash': ''
    }
    block['hash'] = calculate_hash(block['index'], block['timestamp'], block['transactions'], block['previous_hash'])
    chain.append(block)
    save_fn(chain)
    return block['hash']


def add_block_to_blockchain(transaction):
    return _append_block(load_blockchain, save_blockchain, transaction)


def add_block_to_inventory_blockchain(transaction):
    return _append_block(load_inventory_blockchain, save_inventory_blockchain, transaction)


def verify_blockchain():
    chain = load_blockchain()
    for i in range(1, len(chain)):
        curr, prev = chain[i], chain[i - 1]
        if curr['previous_hash'] != prev['hash']:
            return False
        if curr['hash'] != calculate_hash(curr['index'], curr['timestamp'], curr['transactions'], curr['previous_hash']):
            return False
    return True


# ============= UTILITY FUNCTIONS =============

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def sanitize_input(text):
    return bleach.clean(str(text))


def generate_qr_code(data):
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(json.dumps(data))
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    return "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode()


def log_audit(actor_id, actor_type, action, details=''):
    try:
        conn = get_db()
        conn.execute(
            'INSERT INTO audit_logs (actor_id, actor_type, action, details) VALUES (?, ?, ?, ?)',
            (actor_id, actor_type, action, details)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Audit log failed: {e}")


# ============= STATIC PAGE ROUTES =============

@app.route('/')
def landing():
    """Serve the landing page"""
    return send_from_directory(BASE_DIR, 'main.html')


@app.route('/app')
def app_main():
    """Serve the main application"""
    return send_from_directory(BASE_DIR, 'index.html')


@app.route('/favicon.ico')
def favicon():
    return Response(status=204)


# ============= HEALTH CHECK =============

@app.route('/health')
def health():
    """Render uses this to confirm the service is up"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})


# ============= AUTHENTICATION ENDPOINTS =============

@app.route('/api/register/farmer', methods=['POST'])
def register_farmer():
    try:
        data = request.json
        farmer_id = sanitize_input(data['farmer_id'])
        name = sanitize_input(data['name'])
        password = hash_password(data['password'])
        phone = sanitize_input(data.get('phone', ''))
        lga = sanitize_input(data.get('lga', ''))
        ward = sanitize_input(data.get('ward', ''))
        polling_unit = sanitize_input(data.get('polling_unit', ''))
        farm_size = float(data.get('farm_size', 0))

        conn = get_db()
        conn.execute(
            'INSERT INTO farmers (id, name, password, phone, lga, ward, polling_unit, farm_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (farmer_id, name, password, phone, lga, ward, polling_unit, farm_size)
        )
        conn.commit()
        conn.close()

        log_audit(farmer_id, 'farmer', 'register', f'Farmer {name} registered')
        return jsonify({'success': True, 'message': 'Farmer registered successfully'})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': 'Farmer ID already exists'}), 400
    except Exception as e:
        logger.error(f"register_farmer error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/register/admin', methods=['POST'])
def register_admin():
    try:
        data = request.json
        admin_id = sanitize_input(data['admin_id'])
        name = sanitize_input(data['name'])
        password = hash_password(data['password'])

        conn = get_db()
        conn.execute(
            'INSERT INTO admins (id, name, password) VALUES (?, ?, ?)',
            (admin_id, name, password)
        )
        conn.commit()
        conn.close()

        log_audit(admin_id, 'admin', 'register', f'Admin {name} registered')
        return jsonify({'success': True, 'message': 'Admin registered successfully'})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': 'Admin ID already exists'}), 400
    except Exception as e:
        logger.error(f"register_admin error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/register/officer', methods=['POST'])
def register_officer():
    try:
        data = request.json
        officer_id = sanitize_input(data['officer_id'])
        name = sanitize_input(data['name'])
        password = hash_password(data['password'])
        location = sanitize_input(data.get('location', ''))

        conn = get_db()
        conn.execute(
            'INSERT INTO store_officers (id, name, password, location) VALUES (?, ?, ?, ?)',
            (officer_id, name, password, location)
        )
        conn.commit()
        conn.close()

        log_audit(officer_id, 'store_officer', 'register', f'Store Officer {name} registered')
        return jsonify({'success': True, 'message': 'Store Officer registered successfully'})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': 'Officer ID already exists'}), 400
    except Exception as e:
        logger.error(f"register_officer error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/login', methods=['POST'])
def login():
    try:
        data = request.json
        user_id = sanitize_input(data['user_id'])
        password = hash_password(data['password'])

        conn = get_db()
        cursor = conn.cursor()

        if user_id.startswith('F'):
            cursor.execute('SELECT * FROM farmers WHERE id = ? AND password = ?', (user_id, password))
            user_type = 'farmer'
        elif user_id.startswith('A'):
            cursor.execute('SELECT * FROM admins WHERE id = ? AND password = ?', (user_id, password))
            user_type = 'admin'
        elif user_id.startswith('S'):
            cursor.execute('SELECT * FROM store_officers WHERE id = ? AND password = ?', (user_id, password))
            user_type = 'store_officer'
        else:
            conn.close()
            return jsonify({'success': False, 'message': 'Invalid user ID format'}), 400

        user = cursor.fetchone()
        conn.close()

        if user:
            log_audit(user_id, user_type, 'login', 'User logged in')
            return jsonify({'success': True, 'user_type': user_type, 'user_id': user_id, 'name': user['name']})
        else:
            return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
    except Exception as e:
        logger.error(f"login error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= LOCATION MANAGEMENT ENDPOINTS =============

@app.route('/api/locations/lga', methods=['POST'])
def add_lga():
    try:
        name = sanitize_input(request.json['name'])
        conn = get_db()
        conn.execute('INSERT INTO lgas (name) VALUES (?)', (name,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'LGA added successfully'})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'message': 'LGA already exists'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/lga', methods=['GET'])
def get_lgas():
    try:
        conn = get_db()
        lgas = [dict(r) for r in conn.execute('SELECT * FROM lgas ORDER BY name').fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': lgas})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/ward', methods=['POST'])
def add_ward():
    try:
        data = request.json
        name = sanitize_input(data['name'])
        lga_id = int(data['lga_id'])
        conn = get_db()
        conn.execute('INSERT INTO wards (name, lga_id) VALUES (?, ?)', (name, lga_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Ward added successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/ward/<int:lga_id>', methods=['GET'])
def get_wards(lga_id):
    try:
        conn = get_db()
        wards = [dict(r) for r in conn.execute('SELECT * FROM wards WHERE lga_id = ? ORDER BY name', (lga_id,)).fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': wards})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/polling_unit', methods=['POST'])
def add_polling_unit():
    try:
        data = request.json
        name = sanitize_input(data['name'])
        ward_id = int(data['ward_id'])
        conn = get_db()
        conn.execute('INSERT INTO polling_units (name, ward_id) VALUES (?, ?)', (name, ward_id))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Polling unit added successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/polling_unit/<int:ward_id>', methods=['GET'])
def get_polling_units(ward_id):
    try:
        conn = get_db()
        units = [dict(r) for r in conn.execute('SELECT * FROM polling_units WHERE ward_id = ? ORDER BY name', (ward_id,)).fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': units})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= INVENTORY MANAGEMENT ENDPOINTS =============

@app.route('/api/inventory', methods=['POST'])
def add_inventory():
    try:
        data = request.json
        fertilizer_type = sanitize_input(data['fertilizer_type'])
        quantity = int(data['quantity'])
        location = sanitize_input(data.get('location', ''))

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM inventory WHERE fertilizer_type = ? AND location = ?', (fertilizer_type, location))
        existing = cursor.fetchone()

        if existing:
            conn.execute(
                'UPDATE inventory SET quantity = quantity + ?, last_updated = CURRENT_TIMESTAMP WHERE fertilizer_type = ? AND location = ?',
                (quantity, fertilizer_type, location)
            )
        else:
            conn.execute(
                'INSERT INTO inventory (fertilizer_type, quantity, location) VALUES (?, ?, ?)',
                (fertilizer_type, quantity, location)
            )

        conn.commit()
        conn.close()

        add_block_to_inventory_blockchain({
            'type': 'add_inventory',
            'fertilizer_type': fertilizer_type,
            'quantity': quantity,
            'location': location,
            'timestamp': datetime.now().isoformat()
        })

        return jsonify({'success': True, 'message': 'Inventory added successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    try:
        conn = get_db()
        inventory = [dict(r) for r in conn.execute('SELECT * FROM inventory ORDER BY fertilizer_type').fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': inventory})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= SESSION MANAGEMENT ENDPOINTS =============

@app.route('/api/sessions', methods=['POST'])
def create_session():
    try:
        data = request.json
        name = sanitize_input(data['name'])
        fertilizer_type = sanitize_input(data['fertilizer_type'])
        total_bags = int(data['total_bags'])
        start_time = data['start_time']
        end_time = data['end_time']
        created_by = sanitize_input(data['created_by'])

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('SELECT SUM(quantity) as total FROM inventory WHERE fertilizer_type = ?', (fertilizer_type,))
        result = cursor.fetchone()
        available = result['total'] if result['total'] else 0

        if available < total_bags:
            conn.close()
            return jsonify({'success': False, 'message': f'Insufficient inventory. Available: {available} bags'}), 400

        cursor.execute(
            "INSERT INTO sessions (name, fertilizer_type, total_bags, start_time, end_time, created_by, status) VALUES (?, ?, ?, ?, ?, ?, 'active')",
            (name, fertilizer_type, total_bags, start_time, end_time, created_by)
        )
        session_id = cursor.lastrowid
        conn.commit()
        conn.close()

        log_audit(created_by, 'admin', 'create_session', f'Session {name} created with {total_bags} bags')
        return jsonify({'success': True, 'message': 'Session created successfully', 'session_id': session_id})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/sessions', methods=['GET'])
def get_sessions():
    try:
        conn = get_db()
        sessions = [dict(r) for r in conn.execute('SELECT * FROM sessions ORDER BY created_at DESC').fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': sessions})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/sessions/active', methods=['GET'])
def get_active_sessions():
    try:
        conn = get_db()
        sessions = [dict(r) for r in conn.execute(
            "SELECT * FROM sessions WHERE status = 'active' AND datetime(end_time) > datetime('now') ORDER BY created_at DESC"
        ).fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': sessions})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= FARMER REQUEST ENDPOINTS =============

@app.route('/api/requests', methods=['POST'])
def submit_request():
    try:
        data = request.json
        farmer_id = sanitize_input(data['farmer_id'])
        session_id = int(data['session_id'])
        requested_bags = int(data['requested_bags'])

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM sessions WHERE id = ? AND status = "active"', (session_id,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'message': 'Session not found or inactive'}), 400

        cursor.execute('SELECT * FROM farmer_requests WHERE farmer_id = ? AND session_id = ?', (farmer_id, session_id))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'message': 'You already submitted a request for this session'}), 400

        conn.execute(
            "INSERT INTO farmer_requests (farmer_id, session_id, requested_bags, status) VALUES (?, ?, ?, 'pending')",
            (farmer_id, session_id, requested_bags)
        )
        conn.commit()
        conn.close()

        log_audit(farmer_id, 'farmer', 'submit_request', f'Requested {requested_bags} bags for session {session_id}')
        return jsonify({'success': True, 'message': 'Request submitted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/requests/farmer/<farmer_id>', methods=['GET'])
def get_farmer_requests(farmer_id):
    try:
        conn = get_db()
        rows = conn.execute('''
            SELECT r.*, s.name as session_name, s.fertilizer_type
            FROM farmer_requests r
            JOIN sessions s ON r.session_id = s.id
            WHERE r.farmer_id = ?
            ORDER BY r.created_at DESC
        ''', (farmer_id,)).fetchall()
        conn.close()
        return jsonify({'success': True, 'data': [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/requests/session/<int:session_id>', methods=['GET'])
def get_session_requests(session_id):
    try:
        conn = get_db()
        rows = conn.execute('''
            SELECT r.*, f.name as farmer_name, f.farm_size, f.lga, f.ward
            FROM farmer_requests r
            JOIN farmers f ON r.farmer_id = f.id
            WHERE r.session_id = ?
            ORDER BY r.created_at ASC
        ''', (session_id,)).fetchall()
        conn.close()
        return jsonify({'success': True, 'data': [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= ALLOCATION ENDPOINT =============

@app.route('/api/allocate/<int:session_id>', methods=['POST'])
def allocate_fertilizer(session_id):
    try:
        data = request.json
        admin_id = sanitize_input(data['admin_id'])

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM sessions WHERE id = ?', (session_id,))
        session = cursor.fetchone()
        if not session:
            conn.close()
            return jsonify({'success': False, 'message': 'Session not found'}), 404

        cursor.execute('''
            SELECT r.*, f.farm_size, f.total_bags_received
            FROM farmer_requests r
            JOIN farmers f ON r.farmer_id = f.id
            WHERE r.session_id = ? AND r.status = 'pending'
            ORDER BY r.created_at ASC
        ''', (session_id,))
        requests_list = [dict(r) for r in cursor.fetchall()]

        if not requests_list:
            conn.close()
            return jsonify({'success': False, 'message': 'No pending requests for this session'}), 400

        total_bags = session['total_bags']
        remaining_bags = total_bags
        allocations = []

        for req in requests_list:
            if remaining_bags <= 0:
                break

            if req['requested_bags'] <= remaining_bags:
                allocated = req['requested_bags']
            else:
                weight = req['farm_size'] / (req['total_bags_received'] + 1)
                total_weight = sum(r['farm_size'] / (r['total_bags_received'] + 1) for r in requests_list)
                allocated = int((weight / total_weight) * total_bags)
                allocated = min(allocated, req['requested_bags'], remaining_bags)

            remaining_bags -= allocated

            qr_data = {
                'request_id': req['id'],
                'farmer_id': req['farmer_id'],
                'session_id': session_id,
                'allocated_bags': allocated
            }

            blockchain_hash = add_block_to_blockchain({
                'type': 'allocation',
                'request_id': req['id'],
                'farmer_id': req['farmer_id'],
                'session_id': session_id,
                'allocated_bags': allocated,
                'timestamp': datetime.now().isoformat()
            })

            qr_data['blockchain_hash'] = blockchain_hash
            qr_code = generate_qr_code(qr_data)

            conn.execute(
                "UPDATE farmer_requests SET allocated_bags = ?, status = 'approved', qr_code = ?, blockchain_hash = ? WHERE id = ?",
                (allocated, qr_code, blockchain_hash, req['id'])
            )

            allocations.append({'request_id': req['id'], 'farmer_id': req['farmer_id'], 'allocated': allocated})

        conn.execute('UPDATE sessions SET status = "completed" WHERE id = ?', (session_id,))
        conn.execute(
            'UPDATE inventory SET quantity = quantity - ? WHERE fertilizer_type = ?',
            (total_bags - remaining_bags, session['fertilizer_type'])
        )
        conn.commit()
        conn.close()

        log_audit(admin_id, 'admin', 'allocate', f'Allocated fertilizer for session {session_id}')
        return jsonify({'success': True, 'message': f'Allocated successfully. {len(allocations)} farmers approved.', 'allocations': allocations})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= DISTRIBUTION ENDPOINTS =============

@app.route('/api/verify_qr', methods=['POST'])
def verify_qr():
    try:
        qr_data_str = request.json.get('qr_data', '')
        try:
            qr_data = json.loads(qr_data_str)
        except json.JSONDecodeError:
            return jsonify({'success': False, 'message': 'Invalid QR code format'}), 400

        request_id = qr_data.get('request_id')
        blockchain_hash = qr_data.get('blockchain_hash')

        if not request_id or not blockchain_hash:
            return jsonify({'success': False, 'message': 'QR code is missing required information'}), 400

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT r.*, f.name as farmer_name, s.fertilizer_type, s.name as session_name
            FROM farmer_requests r
            JOIN farmers f ON r.farmer_id = f.id
            JOIN sessions s ON r.session_id = s.id
            WHERE r.id = ?
        ''', (request_id,))
        req = cursor.fetchone()
        conn.close()

        if not req:
            return jsonify({'success': False, 'message': 'Request not found in system'}), 404
        if req['blockchain_hash'] != blockchain_hash:
            return jsonify({'success': False, 'message': 'Blockchain verification failed. This may be a fake QR code!'}), 400
        if req['status'] == 'distributed':
            return jsonify({'success': False, 'message': 'This fertilizer has already been distributed'}), 400
        if req['status'] == 'completed':
            return jsonify({'success': False, 'message': 'This transaction is already completed'}), 400
        if req['status'] != 'approved':
            return jsonify({'success': False, 'message': f'Request status is {req["status"]}, not approved for distribution'}), 400

        return jsonify({'success': True, 'data': dict(req)})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Verification error: {str(e)}'}), 500


@app.route('/api/distribute', methods=['POST'])
def distribute_fertilizer():
    try:
        data = request.json
        request_id = int(data['request_id'])
        officer_id = sanitize_input(data['officer_id'])

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM farmer_requests WHERE id = ?', (request_id,))
        req = cursor.fetchone()

        if not req:
            conn.close()
            return jsonify({'success': False, 'message': 'Request not found'}), 404
        if req['status'] != 'approved':
            conn.close()
            return jsonify({'success': False, 'message': 'Request not approved for distribution'}), 400

        conn.execute(
            "UPDATE farmer_requests SET status = 'distributed', distributed_by = ?, distributed_at = CURRENT_TIMESTAMP WHERE id = ?",
            (officer_id, request_id)
        )
        conn.commit()
        conn.close()

        add_block_to_blockchain({
            'type': 'distribution',
            'request_id': request_id,
            'farmer_id': req['farmer_id'],
            'distributed_by': officer_id,
            'allocated_bags': req['allocated_bags'],
            'timestamp': datetime.now().isoformat()
        })

        log_audit(officer_id, 'store_officer', 'distribute', f'Distributed to farmer {req["farmer_id"]}')
        return jsonify({'success': True, 'message': 'Fertilizer distributed successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/acknowledge', methods=['POST'])
def acknowledge_receipt():
    try:
        data = request.json
        request_id = int(data['request_id'])
        farmer_id = sanitize_input(data['farmer_id'])

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM farmer_requests WHERE id = ? AND farmer_id = ?', (request_id, farmer_id))
        req = cursor.fetchone()

        if not req:
            conn.close()
            return jsonify({'success': False, 'message': 'Request not found'}), 404
        if req['status'] != 'distributed':
            conn.close()
            return jsonify({'success': False, 'message': 'Fertilizer not yet distributed'}), 400

        conn.execute(
            "UPDATE farmer_requests SET acknowledged = 1, acknowledged_at = CURRENT_TIMESTAMP, status = 'completed' WHERE id = ?",
            (request_id,)
        )
        conn.execute(
            'UPDATE farmers SET total_bags_received = total_bags_received + ? WHERE id = ?',
            (req['allocated_bags'], farmer_id)
        )
        conn.commit()
        conn.close()

        add_block_to_blockchain({
            'type': 'acknowledgement',
            'request_id': request_id,
            'farmer_id': farmer_id,
            'bags_received': req['allocated_bags'],
            'timestamp': datetime.now().isoformat()
        })

        log_audit(farmer_id, 'farmer', 'acknowledge', f'Acknowledged receipt of {req["allocated_bags"]} bags')
        return jsonify({'success': True, 'message': 'Receipt acknowledged successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= BLOCKCHAIN ENDPOINTS =============

@app.route('/api/blockchain', methods=['GET'])
def get_blockchain():
    try:
        return jsonify({'success': True, 'data': load_blockchain()})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/blockchain/verify', methods=['GET'])
def verify_blockchain_endpoint():
    try:
        is_valid = verify_blockchain()
        return jsonify({'success': True, 'valid': is_valid, 'message': 'Blockchain is valid' if is_valid else 'Blockchain has been tampered with'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= DASHBOARD / STATISTICS ENDPOINTS =============

@app.route('/api/stats/admin', methods=['GET'])
def get_admin_stats():
    try:
        conn = get_db()

        def count(query, params=()):
            return conn.execute(query, params).fetchone()[0] or 0

        total_farmers = count('SELECT COUNT(*) FROM farmers')
        total_admins = count('SELECT COUNT(*) FROM admins')
        total_officers = count('SELECT COUNT(*) FROM store_officers')
        total_sessions = count('SELECT COUNT(*) FROM sessions')
        total_allocated = count('SELECT SUM(allocated_bags) FROM farmer_requests WHERE status != "pending"')
        total_distributed = count('SELECT SUM(allocated_bags) FROM farmer_requests WHERE status IN ("distributed", "completed")')

        request_status = [dict(r) for r in conn.execute('SELECT status, COUNT(*) as count FROM farmer_requests GROUP BY status').fetchall()]
        session_status = [dict(r) for r in conn.execute('SELECT status, COUNT(*) as count FROM sessions GROUP BY status').fetchall()]
        conn.close()

        return jsonify({'success': True, 'data': {
            'total_farmers': total_farmers,
            'total_admins': total_admins,
            'total_officers': total_officers,
            'total_sessions': total_sessions,
            'total_allocated': total_allocated,
            'total_distributed': total_distributed,
            'request_status': request_status,
            'session_status': session_status
        }})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/farmers', methods=['GET'])
def get_all_farmers():
    try:
        conn = get_db()
        farmers = [dict(r) for r in conn.execute(
            'SELECT id, name, phone, lga, ward, polling_unit, farm_size, total_bags_received, created_at FROM farmers ORDER BY name'
        ).fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': farmers})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/officers', methods=['GET'])
def get_all_officers():
    try:
        conn = get_db()
        officers = [dict(r) for r in conn.execute('SELECT id, name, location, created_at FROM store_officers ORDER BY name').fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': officers})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/audit_logs', methods=['GET'])
def get_audit_logs():
    try:
        conn = get_db()
        logs = [dict(r) for r in conn.execute('SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 100').fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': logs})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/distributions/pending', methods=['GET'])
def get_pending_distributions():
    try:
        conn = get_db()
        rows = conn.execute('''
            SELECT r.*, f.name as farmer_name, s.fertilizer_type, s.name as session_name
            FROM farmer_requests r
            JOIN farmers f ON r.farmer_id = f.id
            JOIN sessions s ON r.session_id = s.id
            WHERE r.status = 'approved'
            ORDER BY r.created_at ASC
        ''').fetchall()
        conn.close()
        return jsonify({'success': True, 'data': [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/distributions/officer/<officer_id>', methods=['GET'])
def get_officer_distributions(officer_id):
    try:
        conn = get_db()
        rows = conn.execute('''
            SELECT r.*, f.name as farmer_name, s.fertilizer_type, s.name as session_name
            FROM farmer_requests r
            JOIN farmers f ON r.farmer_id = f.id
            JOIN sessions s ON r.session_id = s.id
            WHERE r.distributed_by = ?
            ORDER BY r.distributed_at DESC
        ''', (officer_id,)).fetchall()
        conn.close()
        return jsonify({'success': True, 'data': [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= CLI COMMANDS =============

@app.cli.command('init-db')
def init_db_command():
    """Run: flask init-db"""
    init_db()
    init_blockchain()
    print("Database and blockchain initialized successfully.")


# ============= APPLICATION ENTRY POINT =============

def bootstrap():
    """Initialize DB and blockchain on first run"""
    if not os.path.exists(DATABASE):
        logger.info("No database found — initializing...")
        init_db()
    if not os.path.exists(BLOCKCHAIN_FILE) or not os.path.exists(INVENTORY_BLOCKCHAIN_FILE):
        logger.info("Blockchain files missing — initializing...")
        init_blockchain()


# Run bootstrap on import (works with gunicorn)
bootstrap()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting ARDA Fertilizer System on port {port}")
    app.run(debug=False, host='0.0.0.0', port=port)
