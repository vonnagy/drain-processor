package drainprocessor.processor.kinesis

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}

/**
 * Created by ivannagy on 4/13/15.
 */
class CredentialsProvider(creds: AWSCredentials) extends AWSCredentialsProvider {

  def getCredentials: AWSCredentials = {
    creds
  }

  def refresh: Unit = {}

}
