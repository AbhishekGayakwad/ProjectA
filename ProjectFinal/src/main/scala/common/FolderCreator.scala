package common

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class FolderCreator(parentFolder: String) {

  // Get the current date formatted as "yyyy-MM-dd"
  private def currentDateString: String = LocalDate.now().format(DateTimeFormatter.ISO_DATE)

  // Method to create the folder with the current date
  def createFolderIfNotExists(): Unit = {
    val parentPath = Paths.get(parentFolder)
    val newFolderPath = parentPath.resolve(currentDateString)

    // Check if the folder exists, if not create it
    if (!Files.exists(newFolderPath)) {
      Files.createDirectory(newFolderPath)
      println(s"Created folder: $newFolderPath")
    } else {
      println(s"Folder already exists: $newFolderPath")
    }
  }
}