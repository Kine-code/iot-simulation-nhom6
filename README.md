Mô phỏng không thay thế hệ thống IoT thực tế mà bổ sung khả năng thử nghiệm các tình huống khó tạo trực tiếp, chẳng hạn hàng trăm thiết bị gửi đồng thời, xác suất mất gói tăng cao hoặc hàng đợi gateway bị giới hạn. Mô hình biểu diễn các bản tin cảm biến phát sinh định kỳ, đi vào một hàng đợi hữu hạn và được truyền theo hai chế độ: best-effort và confirmed.

Best-effort chỉ truyền một lần và không chờ xác nhận. Confirmed bổ sung thời gian xác nhận và cho phép tối đa ba lần thử với cơ chế backoff. Đây là mô hình giáo dục ở mức bản tin, không phải mô phỏng chính xác từng bit của MQTT hoặc CoAP.
