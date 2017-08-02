/*
 * FileName : non_copyable.h
 * Author   : Pengcheng Liu(Lpc-Win32)
 * Date     : Thu 27 Jul 2017 03:46:10 PM CST   Created
*/

#pragma once

namespace pepper
{

    class noncopyable
    {
        protected:
            noncopyable()  = default;
            ~noncopyable() = default;

        private:
            noncopyable(const noncopyable &other) = delete;
            noncopyable &operator =(const noncopyable &other) = delete;
    };

}
