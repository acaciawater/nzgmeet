# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'n871!+1wn@zn+55oe_4^w_xi2ulsw#p1_8nmg_kq-)rty99#-c'
DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.mysql',
        'NAME': 'nzg',
        'USER': 'acacia',
        'PASSWORD': 'Beaumont1',
        'HOST': '',
        'PORT': '',
    }
}
POSTCODE_API_KEY = 'c48b31116d112971df7e669d963f8a9b0c1e8c98'
DEFAULT_FROM_EMAIL = 'noreply@acaciawater.com'
