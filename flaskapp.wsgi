#flaskapp.wsgi
import sys
sys.path.insert(0, '/var/www/html/dbds_proj_1')

from app import app as application