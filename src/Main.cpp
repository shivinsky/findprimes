#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <memory>
#include <fstream>
#include <cmath>

#include <windows.h>

class PrimeFinder;
DWORD WINAPI ThreadProc(LPVOID lpParam);

struct ThreadParam
{
    ThreadParam(PrimeFinder& finder, std::vector<uint64_t>& buffer);

    PrimeFinder& finder;
    std::vector<uint64_t>& buffer;
    uint64_t from;
    uint64_t to;
    uint64_t count;
    uint64_t cycle_size;
};

ThreadParam::ThreadParam(PrimeFinder& finder, std::vector<uint64_t>& buffer)
: finder(finder), buffer(buffer)
{
}

class PrimeFinder
{
public:
    PrimeFinder(uint64_t task, unsigned int parties, const std::string& path);

    uint64_t run();
    void await();
    bool terminated();

    uint64_t find_primes(uint64_t from, uint64_t to,
        std::vector<uint64_t>& primes);
private:
    HANDLE semaphore_;
    unsigned int parties_;
    volatile LONG count_;
    std::vector<std::shared_ptr<ThreadParam>> params_;

    uint64_t task_;
    uint64_t found_;
    bool terminated_;

    std::ofstream output_;
};

PrimeFinder::PrimeFinder(uint64_t task, unsigned int  parties, const std::string& path)
: parties_(parties),
  count_(parties),
  task_(task),
  found_(0),
  terminated_(false),
  output_(path)
{
}

uint64_t PrimeFinder::run()
{
    semaphore_ = CreateSemaphore(0, 0, parties_, 0);

    HANDLE threads[parties_];

    unsigned int max_buffer_size = 1024 * 1024;
    unsigned int cycle_size = std::min(max_buffer_size, static_cast<unsigned int>(task_));
    unsigned int range_size = cycle_size / parties_;

    std::vector<std::vector<uint64_t>> buffer(parties_,
        std::vector<uint64_t>(range_size / (log(range_size) - 1.09f)));
    for (unsigned int i = 0; i < parties_; ++i)
    {
        params_.push_back(std::shared_ptr<ThreadParam>(
            new ThreadParam(*this, buffer[i])));
        params_[i]->from = i * range_size;
        params_[i]->to = i * range_size + range_size;
        params_[i]->cycle_size = cycle_size;
        threads[i] = ::CreateThread(0, 0, ThreadProc, params_[i].get(), 0, 0);
    }

    ::WaitForMultipleObjects(parties_, threads, true, INFINITE);

    for (unsigned int i = 0; i < parties_; ++i)
        ::CloseHandle(threads[i]);
    ::CloseHandle(semaphore_);

    return task_;
}

void PrimeFinder::await()
{
    if (!::InterlockedDecrement(&count_))
    {
        for (unsigned int i = 0; i < parties_ && !terminated_; ++i)
        {
            unsigned int count = params_[i]->count;
            found_ += count;
            if (found_ >= task_)
            {
                count -= found_ - task_;
                terminated_ = true;
            }
            std::copy_n(params_[i]->buffer.begin(), count,
                std::ostream_iterator<uint64_t>(output_, " "));
        }
        count_ = parties_;
        ::ReleaseSemaphore(semaphore_, parties_ - 1, 0);
    }
    else
        ::WaitForSingleObject(semaphore_, INFINITE);
}

bool PrimeFinder::terminated()
{
    return terminated_;
}

uint64_t PrimeFinder::find_primes(uint64_t from, uint64_t to, std::vector<uint64_t>& primes)
{
    std::vector<bool> is_prime((to - from + 1) / 2, true);
    uint64_t root = sqrt(to);

    for (uint64_t i = 3; i <= root; i += 2)
    {
        uint64_t current = std::max(((from + i - 1) / i) * i, i * i);

        if (!(current & 1))
            current += i;

        for (uint64_t j = current; j <= to; j += i * 2)
            is_prime[(j - from) / 2] = false;
    }

    uint64_t current = from;
    if (!(current & 1))
        ++current;

    uint64_t count = 0;
    for (uint64_t i = 0; i < is_prime.size(); ++i)
        if (is_prime[i])
            primes[count++] = current + i * 2;

    if (from <= 2)
        primes[0] = 2;

    return count;
}

DWORD WINAPI ThreadProc(LPVOID lpParam)
{
    ThreadParam* param = static_cast<ThreadParam*>(lpParam);

    while (!param->finder.terminated())
    {
        param->count = param->finder.find_primes(param->from, param->to, param->buffer);
        param->finder.await();
        param->from += param->cycle_size;
        param->to += param->cycle_size;
    }

    return 0;
}

int main()
{
    CHAR buffer[MAX_PATH];
    std::string path(buffer, ::GetModuleFileName(0, buffer, sizeof buffer));
    path = path.substr(0, path.find_last_of("\\/") + 1);

    SYSTEM_INFO system_info;
    ::GetSystemInfo(&system_info);

    PrimeFinder finder(1E3, system_info.dwNumberOfProcessors, path.append("primes.txt"));
    finder.run();
}
