# Online Bank Data URLs TMO
VTB_TMO_URL = 'https://api.louismmoo.com/api/viettinbank/transactions' 
ACB_TMO_URL = 'https://api.louismmoo.com/api/acb/transactions-latest'
BIDV_TMO_URL = 'https://api.louismmoo.com/api/bidv/get-transaction'
VCB_TMO_URL = 'https://api.louismmoo.com/api/vcb/transactions'
EXIM_TMO_URL = 'https://api.louismmoo.com/api/eximbank/transactions'
MBBANK_TMO_URL = 'https://api.louismmoo.com/api/mbbank/transactions'

# Online Bank Data URLs TMO
VCB_ATT_URL = 'http://167.71.208.49/api/history'

# Providers
PROVIDER_NONE = 0
PROVIDER_TMO = 1
PROVIDER_CASSO = 2
PROVIDER_ATT = 3
PROVIDER_KMD = 4

# Provider Psudo Enums
Provider_Value = {
    PROVIDER_NONE: "None",
    PROVIDER_TMO: "TMO",
    PROVIDER_CASSO: "CASSO",
    PROVIDER_ATT: "ATT",
    PROVIDER_KMD: "KMD"
}

# Bank Codes
VTB_CODE = 'VTB'
ACB_CODE = 'ACB'
BIDV_CODE = 'BIDV'
MBB_CODE = 'MBB'
VCB_CODE = 'VCB'
TECH_CODE = 'TECH'
VPBANK_CODE = 'VPBANK'
TIMO_CODE = 'TIMO'
EXIM_CODE = 'EXIM'

# Payment Types
PAYMENT_METHOD_CODE_LBT = "DLBT"
PAYMENT_METHOD_CODE_LBT60 = "DLBT60"
DEFAULT_CURRENCY = "VND"

# Bank Account Status
BANK_ACCOUNT_STATUS_ACTIVE = 1
BANK_ACC_AUTO_STATUS = 2

# Deposit Status
DEPOSIT_STATUS_PROCESSING = 1

# Payment Table Names
BANK_ACCOUNT_TABLE = 'bank_account'
ONLINE_BANK_ACCOUNT_TABLE = 'online_bank_data'  
DEPOSIT_TABLE = 'deposit'
DEPOSIT_LOG_TABLE ='deposit_log'
