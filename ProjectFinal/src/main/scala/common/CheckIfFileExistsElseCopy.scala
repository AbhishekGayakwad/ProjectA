package common

import java.nio.file.{Files, Paths}
import scala.sys.exit

class CheckIfFileExistsElseCopy(path: String, files1: Seq[String], targetPath: String) {

  // Method to check if specified files exist in the folder
  def checkForFiles(): Unit = {
    val parentPath = Paths.get(targetPath)
    // Check if parent folder exists
    if (!Files.exists(parentPath) || !Files.isDirectory(parentPath)) {
      println(s"The folder $parentPath does not exist or is not a directory.")
      return
    }
    // Check each specified file
    files1.foreach { file =>
      val filePath = parentPath.resolve(file)
      if (Files.exists(filePath)) {
        println(s"File '$file' exists in the folder $filePath.")
      } else {
        println(s"File '$file' does not exist in the folder hence copying the file from source $path .")
        copyFilesFromSource()
      }

    }
  }

  def copyFilesFromSource(): Unit = {
    val taPath = Paths.get(targetPath)
    val saPath = Paths.get(path)
    if (!Files.exists(taPath) || !Files.isDirectory(taPath)) {
      println(s"The target path $taPath does not exist or is not a directory.")
      return
    }
    files1.foreach { file =>
      val taFilePath = taPath.resolve(file)
      val saFilePath = saPath.resolve(file)
      if (Files.exists(saFilePath) || !Files.isDirectory(saFilePath)) {
        if (Files.exists(taFilePath) || !Files.isDirectory(taFilePath)) {
          Files.copy(saFilePath, taFilePath)
          println(s"$file copied successfully from $saFilePath to $taFilePath")
        }
        else {
          println(s"The target folder $taFilePath does not exist or is not a directory")
          exit
        }
      }
      else {
        println(s"The source folder $saFilePath does not exist or is not a directory")
        exit
      }

    }


  }


}
