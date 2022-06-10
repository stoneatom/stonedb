/* Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
   Use is subject to license terms

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335 USA
*/
#ifndef STONEDB_SYSTEM_FILE_SYSTEM_H_
#define STONEDB_SYSTEM_FILE_SYSTEM_H_
#pragma once

#include <string>

namespace stonedb {
namespace system {
/** \brief Deletes a directory defined by path
 *  \param path Directory path
 *  The delete operation is recursive. It deletes all existing files and
 * subdirectories
 */
void DeleteDirectory(std::string const &path);

/** \brief Returns file's last content modification time
 *  \param name Path to the file
 *  \return Time typecasted to int
 */
time_t GetFileTime(std::string const &path);

/** \brief Returns file's last metadata modification time
 *  \param name Path to the file
 *  \return Time typecasted to int
 *  \see chown
 */
time_t GetFileCreateTime(std::string const &path);

/** \brief Checks file existense
 *  \param name Path to the file
 *  \return True if a file exists. False if a file doesn't exist
 */
bool DoesFileExist(std::string const &);

/** \brief Rename file
 *  \param OldPath Original file path
 *  \param NewPath New file path
 *  Function throws an exception in case it fails to rename a file
 */
void RenameFile(std::string const &OldPath, std::string const &NewPath);

/** \brief Delete file
 *  \param file  Path to the file
 *  Function throws an exception in case it fails to remove a file
 *  If a file does not exist, no exceptions are thrown
 */
void RemoveFile(std::string const &file, int throwerror = true);

/** \brief Flush to the disk kernel buffers with directory entries changes
 *  \param path  Directory path
 *  Function throws an exception in case it fails to perform
 */
void FlushDirectoryChanges(std::string const &path);

void FlushFileChanges(std::string const &path);

/** \brief Check file permissions
 *  \param path  File path
 *  \return True if a process is allowed to read and write a file. False
 * otherwise
 */
bool IsReadWriteAllowed(std::string const &path);

int ClearDirectory(std::string const &path);
}  // namespace system
}  // namespace stonedb

#endif  // STONEDB_SYSTEM_FILE_SYSTEM_H_
