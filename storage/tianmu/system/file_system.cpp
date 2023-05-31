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

#include "system/file_system.h"

#include <dirent.h>
#include <fcntl.h>
#include <libgen.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>

#include "common/assert.h"
#include "system/tianmu_file.h"
#include "util/fs.h"
#include "util/log_ctl.h"

namespace Tianmu {
namespace system {

int ClearDirectory(std::string const &path) {
  try {
    for (auto &p : fs::directory_iterator(path)) {
      fs::remove_all(p);
    }
  } catch (fs::filesystem_error &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to ClearDirectory(%s). filesystem_error: %s %s %s", path.c_str(), e.what(),
               e.path1().native().c_str(), e.path2().native().c_str());
    return 1;
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to ClearDirectory(%s). error: %s", path.c_str(), e.what());
    return 1;
  }
  return 0;
}

void DeleteDirectory(std::string const &path) {
  std::vector<fs::path> targets;
  try {
    for (auto &p : fs::recursive_directory_iterator(path))
      if (fs::is_symlink(p.path()))
        targets.emplace_back(fs::canonical(fs::absolute(fs::read_symlink(p.path()), p.path().parent_path())));

    fs::remove_all(path);
    std::error_code ec;
    for (auto &t : targets) {
      // ingore error because the target might be under 'path' and thus have
      // been removed
      fs::remove_all(t, ec);
      if (ec && ec != std::errc::no_such_file_or_directory)
        TIANMU_LOG(LogCtl_Level::ERROR, "Failed to delete %s", t.c_str());
    }
  } catch (fs::filesystem_error &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to delete directory(%s). filesystem_error: %s %s %s", path.c_str(),
               e.what(), e.path1().native().c_str(), e.path2().native().c_str());
  } catch (std::exception &e) {
    TIANMU_LOG(LogCtl_Level::ERROR, "Failed to delete directory(%s). error: %s", path.c_str(), e.what());
  }
}

time_t GetFileTime(std::string const &path) {
  struct stat stat_info;
  int ret = stat(path.c_str(), &stat_info);
  if (ret != 0)
    return -1;
  return stat_info.st_mtime;
}

time_t GetFileCreateTime(std::string const &path) {
  struct stat stat_info;
  int ret = stat(path.c_str(), &stat_info);
  if (ret != 0)
    return -1;
  return stat_info.st_ctime;
}

bool DoesFileExist(std::string const &file) {
  struct stat stat_info;
  return (0 == stat(file.c_str(), &stat_info));
}

void RenameFile(std::string const &OldPath, std::string const &NewPath) {
  DEBUG_ASSERT(OldPath.length() && NewPath.length());
  if (rename(OldPath.c_str(), NewPath.c_str())) {
    throw common::DatabaseException(std::strerror(errno));
  }
};

void RemoveFile(std::string const &path, int throwerror) {
  DEBUG_ASSERT(path.length());
  if (remove(path.c_str())) {
    if (DoesFileExist(path) && throwerror) {
      throw common::DatabaseException(std::strerror(errno));
    }
  }
}

void FlushDirectoryChanges(std::string const &path) {
  DIR *dir;
  int res, fd;

  dir = opendir(path.c_str());
  if (dir == 0)
    goto l_err;

  fd = dirfd(dir);
  if (fd < 0)
    goto l_err;

  res = fsync(fd);
  if (res < 0)
    goto l_err;

  closedir(dir);
  return;
l_err:
  if (dir)
    closedir(dir);
  throw common::DatabaseException(std::strerror(errno));
}

void FlushFileChanges(std::string const &path) {
  TianmuFile fb;
  fb.OpenReadOnly(path);
  fb.Flush();
  fb.Close();
}

bool IsReadWriteAllowed(std::string const &path) {
  int ret = access(path.c_str(), R_OK | W_OK);
  return (ret == 0);
}

}  // namespace system
}  // namespace Tianmu
