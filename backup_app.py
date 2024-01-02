# from truckpad.bottle.cors import CorsPlugin
import mysql.connector
from passlib.hash import pbkdf2_sha256
# from flask import Flask, render_template, request, send_file, run, static_file, template, response
from bottle import template, request, run, static_file, template, response, Bottle

# from truckpad.bottle.cors import CorsPlugin
# from truckpad.bottle.cors import CorsPlugin

app = Bottle()

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='admin',
    database='yoandb3'
)
cursor = conn.cursor()


# @app.hook('after_request')
# def enable_cors():
#     response.headers['Access-Control-Allow-Origin'] = 'http://localhost:3000'
#     response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
#     response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'
#
#
# # Route for handling signup
# @app.route('/signup', method=['OPTIONS', 'POST'])
# def signup():
#     if request.method == 'OPTIONS':
#         response.headers['Content-Type'] = 'text/plain'
#         response.headers['Content-Length'] = '0'
#         return ''
#
#     # Assuming the signup data is sent as JSON
#     data = request.json
#
#     # Extract data
#     username = data.get('username')
#     email = data.get('email')
#     raw_password = data.get('password')
#     hashed_password = pbkdf2_sha256.hash(raw_password)
#
#     # Insert data into the users table
#     cursor.execute('INSERT INTO users (username, email, password) VALUES (%s, %s, %s)',
#                    (username, email, hashed_password))
#     conn.commit()
#     return {'status': 'success', 'message': 'User signed up successfully'}
#
#
# # Route for handling login
# @app.route('/login', method=['OPTIONS', 'POST'])
# def login():
#     # This block of code is handling the CORS (Cross-Origin Resource Sharing) preflight request
#     if request.method == 'OPTIONS':
#         response.headers['Content-Type'] = 'text/plain'
#         response.headers['Content-Length'] = '0'
#         return ''
#
#     # Assuming the login data is sent as JSON
#     data = request.json
#
#     # Extract data
#     username = data.get('username')
#     entered_password = data.get('password')
#
#     # Query the users table to retrieve the stored hashed password for the given username
#     cursor.execute('SELECT * FROM users WHERE username = %s', (username,))
#     user = cursor.fetchone()
#     print(cursor)
#
#     if user:
#         stored_password = user[4]  # Assuming hashed password is in the third column
#         print(user)
#
#         # Verify the entered password against the stored hash
#         if pbkdf2_sha256.verify(entered_password, stored_password):
#             # Login successful
#             return {'status': 'success', 'message': 'User logged in successfully'}
#         else:
#             # Login failed
#             return '<p>Invalid login. <a href="/login">Try again</a></p>'
#     else:
#         # Login failed
#         return '<p>Invalid login. <a href="/login">Try again</a></p>'
#
#
# # Static route to serve JavaScript files from the 'static' folder
# @app.route('/upload', method='GET')
# def upload():
#     if request.method == 'OPTIONS':
#         response.headers['Content-Type'] = 'text/plain'
#         response.headers['Content-Length'] = '0'
#         return ''
#     print("hi")
#     return '<h1>Successfully Login</h1>'
#
#
# @app.route('/run-script', methods=['POST'])
# def run_script():
#     if request.method == 'POST':
#         try:
#             # Run your script
#
#             return 'Script executed successfully!'
#         except Exception as e:
#             return f'Error executing script: {e}'
#
@app.route('/test', method='POST')
def index():
    # Access form data using request.form
    job_level = request.forms['job_level']
    job_function = request.forms['job_function']
    country = request.forms['country']
    company_size = request.forms['company_size']
    industry = request.forms['industry']
    suppression = request.forms['suppression']
    tal = request.forms['tal']
    email = request.forms['email']
    job_title_link = request.forms['job_title_link']
    first_last_domain = request.forms['first_last_domain']
    first_last_company = request.forms['first_last_domain']
    print(job_level, job_function, country, company_size, industry, suppression, tal, email,
          job_title_link, first_last_company, first_last_domain)

    return template('test.html', job_level=job_level, job_function=job_function)


@app.route('/favicon.ico')
def ignore_favicon():
    return ""


@app.route('/', methods='POST')
def process_form():
    print("proces")
    return template('user_filter_form.html')


# Install the CorsPlugin after defining all routes
# app.install(CorsPlugin(origins=['http://localhost:3000']))

if __name__ == '__main__':
    run(app, host='localhost', port=8080, debug=True)
