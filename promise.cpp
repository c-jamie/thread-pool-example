#include <future>
#include <iostream>
#include <thread>
#include <chrono>
#include <ctime>
#include <type_traits>


int f(int x) {
	auto start = std::chrono::system_clock::now();
	std::time_t start_time = std::chrono::system_clock::to_time_t(start);
	std::cout << "[ id is: " << std::this_thread::get_id() << "]";
	// std::cout << "returning " << x << " at " << " id " << std::this_thread::get_id() << " - " << std::ctime(&start_time) << std::endl;
	return x;
}


template<typename Function, typename... Args>
auto async(Function&& function, Args&&... args) {

	std::promise<typename std::result_of_t<Function(Args...)>> outer_promise;
	auto future = outer_promise.get_future();
	auto lambda = [function, promise = std::move(outer_promise)](Args&&... args) mutable {
		try {
			promise.set_value(function(args...));
		} catch (...) {
			promise.set_exception(std::current_exception());
		}
	};

	std::thread(std::move(lambda), std::forward<Args>(args)...).detach();
	return future;
}

int main() {

	auto result1 = async(f, 1);	
	auto result2 = async(f, 2);	

	std::cout << "r1 " << result1.get() << std::endl;
	std::cout << "r2 " << result2.get() << std::endl;

}