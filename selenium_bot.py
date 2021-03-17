from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import time
# from databases.utils.utility import timer

username = 'jfalope2016@outlook.com'
password = 'Shoot@34'

AMAZON_URL = 'https://www.amazon.com/gp/product/B08FC5L3RG?pf_rd_r=M6BKQ28E4KT35KWF31BV&pf_rd_p=edaba0ee-c2fe-4124-9f5d-b31d6b1bfbee'
BESTBUY_URL = 'https://www.bestbuy.com/site/sony-playstation-5-digital-edition-console/6430161.p?skuId=6430161'
WALMART_URL = 'https://www.walmart.com/ip/Sony-PlayStation-5-Digital-Edition/493824815'
URLs = [AMAZON_URL, WALMART_URL, BESTBUY_URL]

# element variables
search_element_id = 'global-search-input'
search_submit_element_id = 'global-search-submit'
account_element_id = 'hf-account-flyout'
sign_in_xpath = '//*[@id="sign-in-form"]/button[1]'
email_element_id = 'email'
password_element_id = 'password'
qty_element_id = 'a-autoid-0'

product_name = 'Sony PlayStation 5, Digital Edition'

def bestbuy_purchase_bot(url: str):
    browser = webdriver.Chrome(ChromeDriverManager().install())
    browser.get(url)
    pass


def walmart_bot(url: str):
    browser = webdriver.Chrome(ChromeDriverManager().install())
    log_in_url = 'https://www.walmart.com/account/login?'
    browser.get(url)
    # Log in to Walmart
    browser.get(log_in_url)
    browser.find_element_by_id(email_element_id).send_keys(username)
    browser.find_element_by_id(password_element_id).send_keys(password)
    browser.find_element_by_xpath(sign_in_xpath).click()

    # browser.find_element_by_id(account_element_id).click()

    # search walmart
    browser.get(WALMART_URL)
    browser.find_element_by_id(search_element_id).send_keys(product_name)
    browser.find_element_by_id(search_submit_element_id).click()

    pass


# @timer
def amazon_purchase_bot(url: str):
    browser = webdriver.Chrome(ChromeDriverManager().install())

    browser.get(url)
    try:
        quantity = Select(browser.find_element_by_id('quantity'))
        quantity.select_by_value('2')
        add_to_cart = browser.find_element_by_id('submit.add-to-cart')
        add_to_cart.click()
    except Exception as e:
        print(e)
        print("Product is Unavailable")

    no_coverage = browser.find_element_by_id('a-autoid-4')
    no_coverage.click()

    # browser.close()

    # browser.find_element_by_id('ap_email').send_keys(username)
    # browser.find_element_by_id('continue').click()
    # browser.find_element_by_id('ap_password').send_keys(password)
    # browser.find_element_by_id('auth-signin-button').click()
    # browser.find_element_by_id("hlb-ptc-btn").click()
    # browser.find_element_by_id("submitOrderButtonId-announce").click()
    print('Purchase has been completed')



if __name__ == "__main__":
    test_url = 'https://www.amazon.com/dp/B08231TPSF/ref=s9_acsd_otopr_hd_bw_b73oV9v_c2_x_3_i?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-1&pf_rd_r=8N9HYPBRPG51RP9AMF21&pf_rd_t=101&pf_rd_p=55c5b6f0-ca79-4c95-9c0e-1b275fcd2237&pf_rd_i=6469295011'
    amazon_purchase_bot(url=test_url)
