package com.undertone.dto

import java.sql.Timestamp

case class RequestFunnel(
                         dt:Timestamp,
                         id: String,
                         Flag: String,
                         Action: String,
                         User_Email: String

                        )
