package com.undertone.udms.logsagg.utils

import com.undertone.udmp.utils.{IncrementUtils, IncrementUtilsLambda}

object GetNewFiles {
  def getNewFilesList(bucketName: String,
                          prefix: String,
                          region: String,
                          historyDepth: Int,
                          incrDbUrl: String,
                          incrDbUser: String,
                          incrDbPassword: String,
                          jobName : String,
                          runId: String,
                          isPartitioned: Boolean,
                          fileType: String,
                          datePartitionPrefix: String = "",
                          pathSuffix: String = ""
  ): IncrementUtils={

    val incrementUtil = new IncrementUtils( bucketName=bucketName,
      prefix=prefix,
      region=region,
      historyDepth=historyDepth,
      dbUrl=incrDbUrl,
      dbUsername=incrDbUser,
      dbPassword=incrDbPassword,
      jobName=jobName,
      runId=runId,
      isPartitioned= isPartitioned,
      fileType = fileType,
      datePartitionPrefix = datePartitionPrefix,
      pathSuffix = pathSuffix
    )

    incrementUtil
  }

  def getNewFilesWithLambda(bucketName: String,
                            prefix: String,
                            region: String,
                            historyDepth: Int,
                            dbUrl: String,
                            dbUsername: String,
                            dbPassword: String,
                            jobName : String,
                            runId: String,
                            isPartitioned: Boolean,
                            fileType: String,
                            datePartitionPrefix: String = "",
                            pathSuffix: String = ""
                           ): IncrementUtilsLambda ={
    val incrementUtil = new IncrementUtilsLambda (
      runId=runId,
      bucketName=bucketName,
      prefix=prefix,
      region=region,
      historyDepth=historyDepth,
      dbUrl=dbUrl,
      dbUsername=dbUsername,
      dbPassword=dbPassword,
      jobName=jobName,
      isPartitioned = isPartitioned,
      pathSuffix = "")

    incrementUtil
  }
}
