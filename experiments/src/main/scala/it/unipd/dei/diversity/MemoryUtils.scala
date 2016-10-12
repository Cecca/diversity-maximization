// diversity-maximization: Diversity maximization in Streaming and MapReduce
// Copyright (C) 2016  Matteo Ceccarello <ceccarel@dei.unipd.it>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package it.unipd.dei.diversity


object MemoryUtils {

  def availableBytes: Long = {
    require(Runtime.getRuntime().maxMemory() < Long.MaxValue)
    Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())
  }

  def canAllocate(requiredBytes: Long): Boolean = {
    require(requiredBytes > 0, s"Cannot allocate a negative number of bytes ($requiredBytes)")
    requiredBytes < availableBytes
  }

  def bytesToMegs(numBytes: Long): String = s"${numBytes/1048576L}m"

}
