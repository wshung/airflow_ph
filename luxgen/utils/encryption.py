import hashlib
import base64
import configparser
try:
    from Crypto.Cipher import AES
except:
    pip install pycryptodome
    from Crypto.Cipher import AES

# 定義密碼加密解密所需的 key
key = b'YulonGroupTacKey'  #key: MySuperSecretKey

class TagProjectEncryption:
    def __init__(self, config_keyname):
        self.config_keyname = config_keyname 

    def get_key(self):
        config = configparser.ConfigParser()
        config.read("config/config.ini")
        return config[self.config_keyname]    

    def pad(self, s):
        # 加密時要確保文字長度是 AES 區塊大小的倍數
        return s + (AES.block_size - len(s) % AES.block_size) * chr(AES.block_size - len(s) % AES.block_size)

    def unpad(self, s):
        # 解密後要移除多餘的 padding
        return s[:-ord(s[len(s)-1:])]

    def encrypt_text(self, text):
        # 將文字加密並返回 base64 編碼的結果
        try:
            text = self.pad(text)
            cipher = AES.new(key, AES.MODE_ECB)
            encrypted_text = cipher.encrypt(text.encode('utf-8'))
            base64_entext = base64.b64encode(encrypted_text).decode('utf-8')
        except (Exception) as error:
            print('加密錯誤:', error)
        return base64_entext

    def decrypt_text(self, encrypted_text):
        # 將加密文字解密並返回原始文字
        try:
            encrypted_text = base64.b64decode(encrypted_text)
            cipher = AES.new(key, AES.MODE_ECB)
            decrypted_text = cipher.decrypt(encrypted_text).decode('utf-8')
        except (Exception) as error:
            print('解密錯誤:', error)
        return self.unpad(decrypted_text)

    def encrypt_password(self, password):
        # 使用 SHA-256 對密碼進行加密
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        return self.encrypt_text(password_hash)

    def decrypt_password(self, encrypted_password):
        # 解密密碼並返回原始的 SHA-256 加密的密碼
        decrypted_password = self.decrypt_text(encrypted_password)
        return decrypted_password

if __name__ == "__main__":
    # 初始化加解密物件，傳入密鑰
    main_encryption = TagProjectEncryption(b'TestInfo') 
    print('config_keyname->', main_encryption.config_keyname)

    # 使用 encrypt_password 函式加密密碼
    password = "test"
    encrypted_password = main_encryption.encrypt_text(password)
    print("Encrypted Password:", encrypted_password)

    # 使用 decrypt_password 函式解密密碼
    #encrypted_password = "qow0XCpVPPMZQun5pfZpTA=="
    decrypted_text = main_encryption.decrypt_password(encrypted_password)
    print("Decrypted Password:", decrypted_text)
