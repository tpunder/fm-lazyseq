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
     * 
     * @param maxDepth Using 1 will only return files under the directory that this is being called on
     */
    def recursiveFiles(maxDepth: Int = Int.MaxValue, followSymLinks: Boolean = false, ignoreDotFiles: Boolean = true): LazySeq[File] = {
      recursiveFilesImpl(maxDepth, followSymLinks, includeFiles = true, includeDirs = false, depthFirstDirs = false, ignoreDotFiles = ignoreDotFiles)
    }
    
    /**
     * Recursively list all directories under a directory as a LazySeq
     * 
     * NOTE: This filters out directories or files that start with a dot (.)
     * 
     * @param maxDepth Using 1 will only return dirs under the directory that this is being called on
     */
    def recursiveDirs(maxDepth: Int = Int.MaxValue, followSymLinks: Boolean = false, depthFirstDirs: Boolean = false, ignoreDotFiles: Boolean = true): LazySeq[File] = {
      recursiveFilesImpl(maxDepth, followSymLinks, includeFiles = false, includeDirs = true, depthFirstDirs = depthFirstDirs, ignoreDotFiles = ignoreDotFiles)
    }
    
    /**
     * Recursively list both files and directories under a directory as a LazySeq
     * 
     * NOTE: This filters out directories or files that start with a dot (.)
     * 
     * @param maxDepth Using 1 will only return files and dirs under the directory that this is being called on
     */
    def recursiveFilesAndDirs(maxDepth: Int = Int.MaxValue, followSymLinks: Boolean = false, ignoreDotFiles: Boolean = true, depthFirstDirs: Boolean = false): LazySeq[File] = {
      recursiveFilesImpl(maxDepth, followSymLinks, includeFiles = true, includeDirs = true, depthFirstDirs = depthFirstDirs, ignoreDotFiles = ignoreDotFiles)
    }
    
    private def recursiveFilesImpl(maxDepth: Int, followSymLinks: Boolean, includeFiles: Boolean, includeDirs: Boolean, depthFirstDirs: Boolean, ignoreDotFiles: Boolean): LazySeq[File] = new LazySeq[File] {
      def foreach[U](f: File => U): Unit = {
        require(file.isDirectory, s"Must be a directory: $file")
        
        val visitor = new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            if (!ignore(file)){
              // From the Files.walkFileTree javadocs: 
              //   "The visitFile method is invoked for all files, including directories, encountered at maxDepth,
              //    unless the basic file attributes cannot be read, in which case the visitFileFailed method is invoked"
              if (includeFiles && (attrs.isRegularFile() || attrs.isSymbolicLink())) f(file.toFile())
              if (includeDirs && attrs.isDirectory()) f(file.toFile())
            }
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
            if (ignoreDotFiles) {
              val name: String = path.getFileName.toString
              name.startsWith(".")
            } else {
              false
            }
          }
        }
        
        val options: EnumSet[FileVisitOption] = if (followSymLinks) EnumSet.of(FileVisitOption.FOLLOW_LINKS) else EnumSet.noneOf(classOf[FileVisitOption])
        
        Files.walkFileTree(file.toPath, options, maxDepth, visitor)
      }
    } 
  }
}
