package vmcTest.pages

import geb.navigator.Navigator

import geb.Page

/**
 * Created by anrai on 2/12/15.
 */
class LoginLogoutPage extends Page {
    static url="http://192.168.0.213:8080/"

    static at = {
        waitFor { loginBoxuser1.isDisplayed() }
        waitFor { loginBoxuser2.isDisplayed() }
        waitFor { loginbtn.isDisplayed()      }
    }

    static content = {
        loginBoxuser1 { $("input", type: "text", name: "username") }
        loginBoxuser2 { $("input", type: "password", name: "password") }
        loginbtn { $("input", type: "submit", id: "LoginBtn", value: "Login") }
    }
}
