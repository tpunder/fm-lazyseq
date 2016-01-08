package fm.lazyseq

import java.io.{File, IOException}
import java.nio.file.{Files, FileVisitOption, FileVisitor, FileVisitResult, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import java.util.EnumSet

object Implicits {
  /**
   * Adds a few helpers to java.io.File (which aren't in fm-common since they depend on fm-lazyseq)
   */
  implicit class LazySeqRichFile(val file: File) extends AnyVal {
    /**
     * Recursively list all files under a directory as a LazySeq
     * 
     * NOTE: This filters out directories or files that start with a dot (.)
     */
    def recursiveFiles(maxDepth: Int = Int.MaxValue, followSymLinks: Boolean = false): LazySeq[File] = recursiveFilesAll(maxDepth, followSymLinks, includeFiles = true, includeDirs = false, depthFirstDirs = false)
    
    /**
     * Recursively list all directories under a directory as a LazySeq
     * 
     * NOTE: This filters out directories or files that start with a dot (.)
     */
    def recursiveDirs(maxDepth: Int = Int.MaxValue, followSymLinks: Boolean = false, depthFirstDirs: Boolean = false): LazySeq[File] = recursiveFilesAll(maxDepth, followSymLinks, includeFiles = false, includeDirs = true, depthFirstDirs = depthFirstDirs)
    
    /**
     * Recursively list both files and directories under a directory as a LazySeq
     * 
     * NOTE: This filters out directories or files that start with a dot (.)
     */
    def recursiveFilesAndDirs(maxDepth: Int = Int.MaxValue, followSymLinks: Boolean = false, depthFirstDirs: Boolean = false): LazySeq[File] = recursiveFilesAll(maxDepth, followSymLinks, includeFiles = true, includeDirs = true, depthFirstDirs = depthFirstDirs)
    
    private def recursiveFilesAll(maxDepth: Int, followSymLinks: Boolean, includeFiles: Boolean, includeDirs: Boolean, depthFirstDirs: Boolean): LazySeq[File] = new LazySeq[File] {
      def foreach[U](f: File => U): Unit = {
        require(file.isDirectory, s"Must be a directory: $file")
        
        val visitor = new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            if (!ignore(file) && includeFiles) f(file.toFile())
            super.visitFile(file, attrs)
          }
          
          // Skip the rsync .~tmp~ directory -- This might not even be called (see notes for visitFileFailed())
          override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
            if (ignore(dir)) FileVisitResult.SKIP_SUBTREE
            else {
              if (!depthFirstDirs) visitDir(dir)
              super.preVisitDirectory(dir, attrs)
            }
          }
          
          override def postVisitDirectory(dir: Path, ex: IOException): FileVisitResult = {
            if (depthFirstDirs && !ignore(dir)) visitDir(dir)
            super.postVisitDirectory(dir, ex)
          }
          
          private def visitDir(dir: Path): Unit = if (includeDirs) {
            // Don't include the root directory we are starting from
            val tmp: File = dir.toFile()
            if (tmp != file) f(tmp)
          }
          
          // We don't have access to read the rsync .~tmp~ directory so visitFileFailed will
          // get called for it.
          override def visitFileFailed(file: Path, ex: IOException): FileVisitResult = {
            if (ignore(file)) FileVisitResult.CONTINUE
            else super.visitFileFailed(file, ex)
          }
          
          private def ignore(path: Path): Boolean = {
            val name: String = path.getFileName.toString
            name.startsWith(".")
          }
        }
        
        val options: EnumSet[FileVisitOption] = if (followSymLinks) EnumSet.of(FileVisitOption.FOLLOW_LINKS) else EnumSet.noneOf(classOf[FileVisitOption])
        
        Files.walkFileTree(file.toPath, options, Integer.MAX_VALUE, visitor)
      }
    } 
  }
}
