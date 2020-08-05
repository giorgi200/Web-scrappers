import pymongo
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(ChromeDriverManager().install())
driver.implicitly_wait(5)

def Translate(text):
    try:
        if type(text) == str: 
            driver.get(f"https://translate.google.com/")
            driver.find_element_by_id("source").send_keys(text)

            translated = driver.find_element_by_xpath("/html/body/div[2]/div[2]/div[1]/div[2]/div[1]/div[1]/div[2]/div[3]/div[1]/div[2]/div/span[1]").text
            return translated
        else:
            return ""
    except:
        return ""

