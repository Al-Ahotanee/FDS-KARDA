"""
ARDA Fertilizer Distribution System - Production Ready
PostgreSQL (psycopg v3) + Blockchain + Static Page Serving
Optimized for Render.com deployment
"""

from flask import Flask, request, jsonify, send_file, Response
import psycopg
import psycopg.rows
import hashlib
import json
import os
from datetime import datetime
import qrcode
import io
import base64
import bleach
import logging

# ============= APP CONFIGURATION =============

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'change-this-in-production')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ============= DATABASE CONNECTION =============

def get_db():
    """
    Returns a psycopg v3 connection using DATABASE_URL.
    Render injects DATABASE_URL automatically when a Postgres DB is linked.
    """
    database_url = os.environ.get('DATABASE_URL', '')
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    conn = psycopg.connect(database_url, row_factory=psycopg.rows.dict_row)
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_db()
    with conn.cursor() as cur:

        cur.execute('''
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

        cur.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS store_officers (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                password TEXT NOT NULL,
                location TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY,
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

        cur.execute('''
            CREATE TABLE IF NOT EXISTS farmer_requests (
                id SERIAL PRIMARY KEY,
                farmer_id TEXT NOT NULL REFERENCES farmers(id),
                session_id INTEGER NOT NULL REFERENCES sessions(id),
                requested_bags INTEGER NOT NULL,
                allocated_bags INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                qr_code TEXT,
                blockchain_hash TEXT,
                distributed_by TEXT,
                distributed_at TIMESTAMP,
                acknowledged BOOLEAN DEFAULT FALSE,
                acknowledged_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                fertilizer_type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit TEXT DEFAULT 'bags',
                location TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS lgas (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS wards (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                lga_id INTEGER NOT NULL REFERENCES lgas(id)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS polling_units (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                ward_id INTEGER NOT NULL REFERENCES wards(id)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                actor_id TEXT NOT NULL,
                actor_type TEXT NOT NULL,
                action TEXT NOT NULL,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS blockchain (
                id SERIAL PRIMARY KEY,
                chain_name TEXT NOT NULL,
                block_index INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                transactions JSONB NOT NULL,
                previous_hash TEXT NOT NULL,
                hash TEXT NOT NULL
            )
        ''')

    conn.commit()
    conn.close()
    logger.info("PostgreSQL database initialized successfully.")


# ============= SEED DEFAULT CREDENTIALS =============

def seed_defaults():
    """
    Insert default users on first boot if they don't already exist.
    Credentials:
      Admin:         A001 / Admin
      Store Officer: S001 / Officer
      Farmer:        F001 / Farmer
    """
    defaults = [
        {
            'table': 'admins',
            'insert': 'INSERT INTO admins (id, name, password) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING',
            'values': ('A001', 'Admin', hash_password('Admin')),
            'label': 'Admin A001',
        },
        {
            'table': 'store_officers',
            'insert': 'INSERT INTO store_officers (id, name, password, location) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING',
            'values': ('S001', 'Store Officer', hash_password('Officer'), 'HQ'),
            'label': 'Store Officer S001',
        },
        {
            'table': 'farmers',
            'insert': 'INSERT INTO farmers (id, name, password, phone, lga, ward, polling_unit, farm_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING',
            'values': ('F001', 'Demo Farmer', hash_password('Farmer'), '08000000000', 'Katsina', 'Central', 'Unit 1', 2.5),
            'label': 'Farmer F001',
        },
    ]

    conn = get_db()
    with conn.cursor() as cur:
        for user in defaults:
            cur.execute(user['insert'], user['values'])
            logger.info(f"Seeded default user: {user['label']}")
    conn.commit()
    conn.close()


# ============= BLOCKCHAIN FUNCTIONS =============

def calculate_hash(index, timestamp, transactions, previous_hash):
    value = str(index) + str(timestamp) + json.dumps(transactions, sort_keys=True) + str(previous_hash)
    return hashlib.sha256(value.encode()).hexdigest()


def init_blockchain():
    """Insert genesis blocks for both chains if they don't exist."""
    conn = get_db()
    with conn.cursor() as cur:
        for chain_name in ('distribution', 'inventory'):
            cur.execute('SELECT COUNT(*) as count FROM blockchain WHERE chain_name = %s', (chain_name,))
            if cur.fetchone()['count'] == 0:
                ts = datetime.now().isoformat()
                genesis_hash = calculate_hash(0, ts, [], '0')
                cur.execute(
                    'INSERT INTO blockchain (chain_name, block_index, timestamp, transactions, previous_hash, hash) VALUES (%s, %s, %s, %s, %s, %s)',
                    (chain_name, 0, ts, json.dumps([]), '0', genesis_hash)
                )
                logger.info(f"{chain_name.capitalize()} blockchain genesis block created.")
    conn.commit()
    conn.close()


def _load_chain(chain_name):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM blockchain WHERE chain_name = %s ORDER BY block_index ASC', (chain_name,))
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _append_block(chain_name, transaction):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM blockchain WHERE chain_name = %s ORDER BY block_index DESC LIMIT 1', (chain_name,))
        prev = dict(cur.fetchone())
        ts = datetime.now().isoformat()
        transactions = [transaction]
        new_index = prev['block_index'] + 1
        new_hash = calculate_hash(new_index, ts, transactions, prev['hash'])
        cur.execute(
            'INSERT INTO blockchain (chain_name, block_index, timestamp, transactions, previous_hash, hash) VALUES (%s, %s, %s, %s, %s, %s)',
            (chain_name, new_index, ts, json.dumps(transactions), prev['hash'], new_hash)
        )
    conn.commit()
    conn.close()
    return new_hash


def add_block_to_blockchain(transaction):
    return _append_block('distribution', transaction)


def add_block_to_inventory_blockchain(transaction):
    return _append_block('inventory', transaction)


def load_blockchain():
    return _load_chain('distribution')


def verify_blockchain():
    chain = _load_chain('distribution')
    for i in range(1, len(chain)):
        curr, prev = chain[i], chain[i - 1]
        if curr['previous_hash'] != prev['hash']:
            return False
        txns = curr['transactions'] if isinstance(curr['transactions'], list) else json.loads(curr['transactions'])
        if curr['hash'] != calculate_hash(curr['block_index'], curr['timestamp'], txns, curr['previous_hash']):
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
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO audit_logs (actor_id, actor_type, action, details) VALUES (%s, %s, %s, %s)',
                (actor_id, actor_type, action, details)
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Audit log failed: {e}")


# ============= STATIC PAGE ROUTES =============

def resolve_file(filename):
    for directory in [BASE_DIR, os.getcwd(), '/opt/render/project/src']:
        path = os.path.join(directory, filename)
        if os.path.isfile(path):
            return path
    return None


@app.route('/')
def landing():
    path = resolve_file('main.html') or resolve_file('landing.html')
    if path:
        return send_file(path)
    return jsonify({'error': 'Landing page not found. Add main.html or landing.html to repo root.'}), 404


@app.route('/app')
def app_main():
    path = resolve_file('index.html')
    if path:
        return send_file(path)
    return jsonify({'error': 'index.html not found. Ensure it is committed to repo root.'}), 404


@app.route('/favicon.ico')
def favicon():
    return Response(status=204)


# ============= HEALTH CHECK =============

@app.route('/health')
def health():
    try:
        conn = get_db()
        conn.close()
        db_status = 'connected'
    except Exception as e:
        db_status = f'error: {str(e)}'
    return jsonify({'status': 'ok', 'database': db_status, 'timestamp': datetime.now().isoformat()})


# ============= AUTHENTICATION ENDPOINTS =============

@app.route('/api/register/farmer', methods=['POST'])
def register_farmer():
    try:
        data         = request.json
        farmer_id    = sanitize_input(data['farmer_id'])
        name         = sanitize_input(data['name'])
        password     = hash_password(data['password'])
        phone        = sanitize_input(data.get('phone', ''))
        lga          = sanitize_input(data.get('lga', ''))
        ward         = sanitize_input(data.get('ward', ''))
        polling_unit = sanitize_input(data.get('polling_unit', ''))
        farm_size    = float(data.get('farm_size', 0))

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO farmers (id, name, password, phone, lga, ward, polling_unit, farm_size) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
                (farmer_id, name, password, phone, lga, ward, polling_unit, farm_size)
            )
        conn.commit()
        conn.close()
        log_audit(farmer_id, 'farmer', 'register', f'Farmer {name} registered')
        return jsonify({'success': True, 'message': 'Farmer registered successfully'})
    except psycopg.errors.UniqueViolation:
        return jsonify({'success': False, 'message': 'Farmer ID already exists'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/register/admin', methods=['POST'])
def register_admin():
    try:
        data     = request.json
        admin_id = sanitize_input(data['admin_id'])
        name     = sanitize_input(data['name'])
        password = hash_password(data['password'])
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('INSERT INTO admins (id, name, password) VALUES (%s, %s, %s)', (admin_id, name, password))
        conn.commit()
        conn.close()
        log_audit(admin_id, 'admin', 'register', f'Admin {name} registered')
        return jsonify({'success': True, 'message': 'Admin registered successfully'})
    except psycopg.errors.UniqueViolation:
        return jsonify({'success': False, 'message': 'Admin ID already exists'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/register/officer', methods=['POST'])
def register_officer():
    try:
        data       = request.json
        officer_id = sanitize_input(data['officer_id'])
        name       = sanitize_input(data['name'])
        password   = hash_password(data['password'])
        location   = sanitize_input(data.get('location', ''))
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO store_officers (id, name, password, location) VALUES (%s, %s, %s, %s)',
                (officer_id, name, password, location)
            )
        conn.commit()
        conn.close()
        log_audit(officer_id, 'store_officer', 'register', f'Store Officer {name} registered')
        return jsonify({'success': True, 'message': 'Store Officer registered successfully'})
    except psycopg.errors.UniqueViolation:
        return jsonify({'success': False, 'message': 'Officer ID already exists'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/login', methods=['POST'])
def login():
    try:
        data     = request.json
        user_id  = sanitize_input(data['user_id'])
        password = hash_password(data['password'])
        conn = get_db()
        with conn.cursor() as cur:
            if user_id.startswith('F'):
                cur.execute('SELECT * FROM farmers WHERE id = %s AND password = %s', (user_id, password))
                user_type = 'farmer'
            elif user_id.startswith('A'):
                cur.execute('SELECT * FROM admins WHERE id = %s AND password = %s', (user_id, password))
                user_type = 'admin'
            elif user_id.startswith('S'):
                cur.execute('SELECT * FROM store_officers WHERE id = %s AND password = %s', (user_id, password))
                user_type = 'store_officer'
            else:
                conn.close()
                return jsonify({'success': False, 'message': 'Invalid user ID format'}), 400
            user = cur.fetchone()
        conn.close()

        if user:
            log_audit(user_id, user_type, 'login', 'User logged in')
            return jsonify({'success': True, 'user_type': user_type, 'user_id': user_id, 'name': user['name']})
        return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= LOCATION MANAGEMENT ENDPOINTS =============

@app.route('/api/locations/lga', methods=['POST'])
def add_lga():
    try:
        name = sanitize_input(request.json['name'])
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('INSERT INTO lgas (name) VALUES (%s)', (name,))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'LGA added successfully'})
    except psycopg.errors.UniqueViolation:
        return jsonify({'success': False, 'message': 'LGA already exists'}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/lga', methods=['GET'])
def get_lgas():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM lgas ORDER BY name')
            lgas = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': lgas})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/ward', methods=['POST'])
def add_ward():
    try:
        data = request.json
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('INSERT INTO wards (name, lga_id) VALUES (%s, %s)', (sanitize_input(data['name']), int(data['lga_id'])))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Ward added successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/ward/<int:lga_id>', methods=['GET'])
def get_wards(lga_id):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM wards WHERE lga_id = %s ORDER BY name', (lga_id,))
            wards = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': wards})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/polling_unit', methods=['POST'])
def add_polling_unit():
    try:
        data = request.json
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('INSERT INTO polling_units (name, ward_id) VALUES (%s, %s)', (sanitize_input(data['name']), int(data['ward_id'])))
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Polling unit added successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/locations/polling_unit/<int:ward_id>', methods=['GET'])
def get_polling_units(ward_id):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM polling_units WHERE ward_id = %s ORDER BY name', (ward_id,))
            units = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': units})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= INVENTORY MANAGEMENT ENDPOINTS =============

@app.route('/api/inventory', methods=['POST'])
def add_inventory():
    try:
        data            = request.json
        fertilizer_type = sanitize_input(data['fertilizer_type'])
        quantity        = int(data['quantity'])
        location        = sanitize_input(data.get('location', ''))

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT id FROM inventory WHERE fertilizer_type = %s AND location = %s', (fertilizer_type, location))
            if cur.fetchone():
                cur.execute(
                    'UPDATE inventory SET quantity = quantity + %s, last_updated = CURRENT_TIMESTAMP WHERE fertilizer_type = %s AND location = %s',
                    (quantity, fertilizer_type, location)
                )
            else:
                cur.execute('INSERT INTO inventory (fertilizer_type, quantity, location) VALUES (%s, %s, %s)', (fertilizer_type, quantity, location))
        conn.commit()
        conn.close()

        add_block_to_inventory_blockchain({
            'type': 'add_inventory', 'fertilizer_type': fertilizer_type,
            'quantity': quantity, 'location': location, 'timestamp': datetime.now().isoformat()
        })
        return jsonify({'success': True, 'message': 'Inventory added successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM inventory ORDER BY fertilizer_type')
            inventory = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': inventory})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= SESSION MANAGEMENT ENDPOINTS =============

@app.route('/api/sessions', methods=['POST'])
def create_session():
    try:
        data            = request.json
        name            = sanitize_input(data['name'])
        fertilizer_type = sanitize_input(data['fertilizer_type'])
        total_bags      = int(data['total_bags'])
        start_time      = data['start_time']
        end_time        = data['end_time']
        created_by      = sanitize_input(data['created_by'])

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT COALESCE(SUM(quantity), 0) as total FROM inventory WHERE fertilizer_type = %s', (fertilizer_type,))
            available = cur.fetchone()['total']
            if available < total_bags:
                conn.close()
                return jsonify({'success': False, 'message': f'Insufficient inventory. Available: {available} bags'}), 400
            cur.execute(
                "INSERT INTO sessions (name, fertilizer_type, total_bags, start_time, end_time, created_by, status) VALUES (%s, %s, %s, %s, %s, %s, 'active') RETURNING id",
                (name, fertilizer_type, total_bags, start_time, end_time, created_by)
            )
            session_id = cur.fetchone()['id']
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
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM sessions ORDER BY created_at DESC')
            sessions = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': sessions})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/sessions/active', methods=['GET'])
def get_active_sessions():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sessions WHERE status = 'active' AND end_time > NOW() ORDER BY created_at DESC")
            sessions = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': sessions})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= FARMER REQUEST ENDPOINTS =============

@app.route('/api/requests', methods=['POST'])
def submit_request():
    try:
        data           = request.json
        farmer_id      = sanitize_input(data['farmer_id'])
        session_id     = int(data['session_id'])
        requested_bags = int(data['requested_bags'])

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM sessions WHERE id = %s AND status = 'active'", (session_id,))
            if not cur.fetchone():
                conn.close()
                return jsonify({'success': False, 'message': 'Session not found or inactive'}), 400
            cur.execute('SELECT id FROM farmer_requests WHERE farmer_id = %s AND session_id = %s', (farmer_id, session_id))
            if cur.fetchone():
                conn.close()
                return jsonify({'success': False, 'message': 'You already submitted a request for this session'}), 400
            cur.execute(
                "INSERT INTO farmer_requests (farmer_id, session_id, requested_bags, status) VALUES (%s, %s, %s, 'pending')",
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
        with conn.cursor() as cur:
            cur.execute('''
                SELECT r.*, s.name as session_name, s.fertilizer_type
                FROM farmer_requests r JOIN sessions s ON r.session_id = s.id
                WHERE r.farmer_id = %s ORDER BY r.created_at DESC
            ''', (farmer_id,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/requests/session/<int:session_id>', methods=['GET'])
def get_session_requests(session_id):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('''
                SELECT r.*, f.name as farmer_name, f.farm_size, f.lga, f.ward
                FROM farmer_requests r JOIN farmers f ON r.farmer_id = f.id
                WHERE r.session_id = %s ORDER BY r.created_at ASC
            ''', (session_id,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= ALLOCATION ENDPOINT =============

@app.route('/api/allocate/<int:session_id>', methods=['POST'])
def allocate_fertilizer(session_id):
    try:
        admin_id = sanitize_input(request.json['admin_id'])
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM sessions WHERE id = %s', (session_id,))
            session = cur.fetchone()
            if not session:
                conn.close()
                return jsonify({'success': False, 'message': 'Session not found'}), 404

            cur.execute('''
                SELECT r.*, f.farm_size, f.total_bags_received
                FROM farmer_requests r JOIN farmers f ON r.farmer_id = f.id
                WHERE r.session_id = %s AND r.status = 'pending' ORDER BY r.created_at ASC
            ''', (session_id,))
            requests_list = [dict(r) for r in cur.fetchall()]

            if not requests_list:
                conn.close()
                return jsonify({'success': False, 'message': 'No pending requests for this session'}), 400

            total_bags     = session['total_bags']
            remaining_bags = total_bags
            allocations    = []

            for req in requests_list:
                if remaining_bags <= 0:
                    break
                if req['requested_bags'] <= remaining_bags:
                    allocated = req['requested_bags']
                else:
                    weight       = req['farm_size'] / (req['total_bags_received'] + 1)
                    total_weight = sum(r['farm_size'] / (r['total_bags_received'] + 1) for r in requests_list)
                    allocated    = int((weight / total_weight) * total_bags)
                    allocated    = min(allocated, req['requested_bags'], remaining_bags)

                remaining_bags -= allocated

                qr_data = {
                    'request_id': req['id'], 'farmer_id': req['farmer_id'],
                    'session_id': session_id, 'allocated_bags': allocated
                }
                blockchain_hash = add_block_to_blockchain({
                    'type': 'allocation', 'request_id': req['id'],
                    'farmer_id': req['farmer_id'], 'session_id': session_id,
                    'allocated_bags': allocated, 'timestamp': datetime.now().isoformat()
                })
                qr_data['blockchain_hash'] = blockchain_hash
                qr_code = generate_qr_code(qr_data)

                cur.execute(
                    "UPDATE farmer_requests SET allocated_bags = %s, status = 'approved', qr_code = %s, blockchain_hash = %s WHERE id = %s",
                    (allocated, qr_code, blockchain_hash, req['id'])
                )
                allocations.append({'request_id': req['id'], 'farmer_id': req['farmer_id'], 'allocated': allocated})

            cur.execute("UPDATE sessions SET status = 'completed' WHERE id = %s", (session_id,))
            cur.execute(
                'UPDATE inventory SET quantity = quantity - %s WHERE fertilizer_type = %s',
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

        request_id      = qr_data.get('request_id')
        blockchain_hash = qr_data.get('blockchain_hash')
        if not request_id or not blockchain_hash:
            return jsonify({'success': False, 'message': 'QR code is missing required information'}), 400

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('''
                SELECT r.*, f.name as farmer_name, s.fertilizer_type, s.name as session_name
                FROM farmer_requests r
                JOIN farmers f ON r.farmer_id = f.id
                JOIN sessions s ON r.session_id = s.id
                WHERE r.id = %s
            ''', (request_id,))
            req = cur.fetchone()
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
        data       = request.json
        request_id = int(data['request_id'])
        officer_id = sanitize_input(data['officer_id'])

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM farmer_requests WHERE id = %s', (request_id,))
            req = cur.fetchone()
            if not req:
                conn.close()
                return jsonify({'success': False, 'message': 'Request not found'}), 404
            if req['status'] != 'approved':
                conn.close()
                return jsonify({'success': False, 'message': 'Request not approved for distribution'}), 400
            cur.execute(
                "UPDATE farmer_requests SET status = 'distributed', distributed_by = %s, distributed_at = CURRENT_TIMESTAMP WHERE id = %s",
                (officer_id, request_id)
            )
        conn.commit()
        conn.close()
        add_block_to_blockchain({
            'type': 'distribution', 'request_id': request_id,
            'farmer_id': req['farmer_id'], 'distributed_by': officer_id,
            'allocated_bags': req['allocated_bags'], 'timestamp': datetime.now().isoformat()
        })
        log_audit(officer_id, 'store_officer', 'distribute', f'Distributed to farmer {req["farmer_id"]}')
        return jsonify({'success': True, 'message': 'Fertilizer distributed successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/acknowledge', methods=['POST'])
def acknowledge_receipt():
    try:
        data       = request.json
        request_id = int(data['request_id'])
        farmer_id  = sanitize_input(data['farmer_id'])

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM farmer_requests WHERE id = %s AND farmer_id = %s', (request_id, farmer_id))
            req = cur.fetchone()
            if not req:
                conn.close()
                return jsonify({'success': False, 'message': 'Request not found'}), 404
            if req['status'] != 'distributed':
                conn.close()
                return jsonify({'success': False, 'message': 'Fertilizer not yet distributed'}), 400
            cur.execute(
                "UPDATE farmer_requests SET acknowledged = TRUE, acknowledged_at = CURRENT_TIMESTAMP, status = 'completed' WHERE id = %s",
                (request_id,)
            )
            cur.execute(
                'UPDATE farmers SET total_bags_received = total_bags_received + %s WHERE id = %s',
                (req['allocated_bags'], farmer_id)
            )
        conn.commit()
        conn.close()
        add_block_to_blockchain({
            'type': 'acknowledgement', 'request_id': request_id,
            'farmer_id': farmer_id, 'bags_received': req['allocated_bags'],
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
        with conn.cursor() as cur:
            def scalar(q, p=()):
                cur.execute(q, p)
                return list(cur.fetchone().values())[0] or 0

            stats = {
                'total_farmers':     scalar('SELECT COUNT(*) FROM farmers'),
                'total_admins':      scalar('SELECT COUNT(*) FROM admins'),
                'total_officers':    scalar('SELECT COUNT(*) FROM store_officers'),
                'total_sessions':    scalar('SELECT COUNT(*) FROM sessions'),
                'total_allocated':   scalar("SELECT COALESCE(SUM(allocated_bags),0) FROM farmer_requests WHERE status != 'pending'"),
                'total_distributed': scalar("SELECT COALESCE(SUM(allocated_bags),0) FROM farmer_requests WHERE status IN ('distributed','completed')"),
            }
            cur.execute('SELECT status, COUNT(*) as count FROM farmer_requests GROUP BY status')
            stats['request_status'] = [dict(r) for r in cur.fetchall()]
            cur.execute('SELECT status, COUNT(*) as count FROM sessions GROUP BY status')
            stats['session_status'] = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': stats})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/farmers', methods=['GET'])
def get_all_farmers():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT id, name, phone, lga, ward, polling_unit, farm_size, total_bags_received, created_at FROM farmers ORDER BY name')
            farmers = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': farmers})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/officers', methods=['GET'])
def get_all_officers():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT id, name, location, created_at FROM store_officers ORDER BY name')
            officers = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': officers})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/audit_logs', methods=['GET'])
def get_audit_logs():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 100')
            logs = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': logs})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/distributions/pending', methods=['GET'])
def get_pending_distributions():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('''
                SELECT r.*, f.name as farmer_name, s.fertilizer_type, s.name as session_name
                FROM farmer_requests r JOIN farmers f ON r.farmer_id = f.id JOIN sessions s ON r.session_id = s.id
                WHERE r.status = 'approved' ORDER BY r.created_at ASC
            ''')
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/distributions/officer/<officer_id>', methods=['GET'])
def get_officer_distributions(officer_id):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute('''
                SELECT r.*, f.name as farmer_name, s.fertilizer_type, s.name as session_name
                FROM farmer_requests r JOIN farmers f ON r.farmer_id = f.id JOIN sessions s ON r.session_id = s.id
                WHERE r.distributed_by = %s ORDER BY r.distributed_at DESC
            ''', (officer_id,))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ============= CLI COMMAND =============

@app.cli.command('init-db')
def init_db_command():
    """Run: flask init-db"""
    init_db()
    init_blockchain()
    seed_defaults()
    print("Database, blockchain, and default credentials initialized.")


# ============= BOOTSTRAP =============

def bootstrap():
    """Auto-create tables, blockchain, and seed default users on every startup."""
    try:
        init_db()
        init_blockchain()
        seed_defaults()
        logger.info("Bootstrap complete.")
    except Exception as e:
        logger.error(f"Bootstrap failed (DATABASE_URL may not be set): {e}")


bootstrap()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting ARDA on port {port}")
    app.run(debug=False, host='0.0.0.0', port=port)
