package pack

import MySqlManager.*

fun main() {
    val connMsg = MySqlConnMsg("localhost", 3306, "root", "65a1561425f744e2b541303f628963f8")
    println("????????????????????")
    val msm = MySqlManager(connMsg, "pinn")
    val table = msm.GetTable("SELECT * FROM historia")
    for (el in table) {
        println(el.get(1))
    }
    println("????????????????????")
}