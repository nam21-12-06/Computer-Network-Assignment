# Phần việc Tracker Peer Discovery và Channel Management

Tài liệu này giải thích phần code dành cho Nguyễn Trọng Nhân. Dữ liệu tracker
hiện lưu trong bộ nhớ; khi khởi động lại tiến trình, peer và channel cần đăng ký lại.

## Danh sách file đã tạo hoặc sửa

- `apps/tracker.py`: danh sách peer, heartbeat, hết hạn, channel và thành viên.
- `apps/tracker_app.py`: các route HTTP JSON gọi vào `TrackerService`.
- `apps/peer.py`: client tracker và peer bất đồng bộ, kết nối trực tiếp TCP.
- `start_tracker_app.py`: khởi động app tracker thông qua StreapRous.
- `start_peer.py`: khởi động peer và nhận frame; `--connect` chọn peer đích.
- `daemon/request.py`: tách header và body để route đọc JSON.
- `daemon/httpadapter.py`: gọi route và tạo/gửi response HTTP.
- `apps/__init__.py`: import chậm để có thể dùng service độc lập.
- `streaprous_socket/streaprous_raw_connection.py`: cho phép thiếu netifaces khi import;
  khi có netifaces, code vẫn dùng nó tìm interface như trước.
- `tests/test_tracker.py`, `tests/test_tracker_app.py`, `tests/test_http_routes.py`,
  `tests/test_peer.py`, `tests/test_tracker_http.py`: kiểm thử service, HTTP và peer.

## Luồng hoạt động

1. `PeerNode.start()` mở cổng TCP trước, sau đó gửi đăng ký đến tracker.
2. Peer gửi heartbeat định kỳ; mặc định mỗi 20 giây, tracker dùng TTL 60 giây.
3. `discover()` hỏi tracker danh sách peer đang hoạt động.
4. `connect_peer()` hỏi endpoint, mở TCP trực tiếp và trao đổi frame `hello`.
5. `send()` gửi một JSON object qua kết nối trực tiếp; đầu nhận lấy từ `incoming`.
6. `disconnect_peer()` đóng một kết nối; `close()` dừng listener, đóng các kết nối,
   ngừng heartbeat và báo tracker. Nếu không báo được, tracker dùng TTL.

Frame là JSON UTF-8 kết thúc bằng newline, tối đa 65536 byte kể cả newline.
Đây là protocol minh họa để tích hợp; nhóm Huy cần thống nhất format tin nhắn.
`peer_id` trong handshake chưa được xác thực và cần nối với phần đăng nhập của nhóm.

Channel dùng `peer_id` làm định danh thành viên. Tạo, join, leave, liệt kê channel
của peer và thành viên qua các route `/channels/create`, `/channels/join`,
`/channels/leave`, `/channels/mine`, `/channels/members`. Khi chủ rời, quyền sở hữu
chuyển sang peer còn lại có ID nhỏ nhất; channel rỗng bị xóa. Thành viên offline
vẫn thuộc channel nhưng không xuất hiện trong danh sách thành viên active.

Tracker xử lý hết hạn khi truy vấn discovery, connect, members hoặc gọi
`cleanup_expired_peers()`; nó không tạo một luồng dọn nền. Cách này bảo đảm các
danh sách trên được cập nhật khi người dùng truy vấn.

## Thử trên Linux

Python 3.10 trở lên; code tracker/peer mới dùng thư viện chuẩn. `netifaces` là
thư viện code gốc dùng để phát hiện interface; nên cài trên VM nếu chưa có.
Chạy kiểm thử từ thư mục gốc repo:

```bash
python3 -m unittest discover -s tests -v
```

Khởi động tracker qua backend raw socket của repo, với quyền cần thiết cho raw
socket và dùng IP thực tế của VM:

```bash
sudo python3 start_tracker_app.py --server-ip 192.168.56.10 --server-port 2027
```

Trên các máy peer, sau khi tracker sẵn sàng, ví dụ:

```bash
python3 start_peer.py --peer-id nhan --user-id nhan --tracker http://192.168.56.10:2027 --listen-port 9100
python3 start_peer.py --peer-id huy --user-id huy --tracker http://192.168.56.10:2027 --listen-port 9101 --connect nhan
```

Mở cổng và chọn IP theo mạng VM thực tế. API `PeerNode.send()` là điểm nhóm
messaging gọi để gửi frame. CLI hiện chỉ hiển thị frame nhận, không phải chat UI.

## Phạm vi kiểm thử và phần cần tích hợp

`tests/test_peer.py` dùng socket TCP thật và tracker giả trong cùng tiến trình.
`tests/test_tracker_http.py` dùng HTTP thật trên loopback, route StreapRous và
hai peer TCP, nhưng máy chủ HTTP trong test là `http.server` để tách riêng logic.
Hai test này không chứng minh backend raw socket của thầy hoạt động đúng.

Peer hiện sử dụng `asyncio` TCP stream của Python; nhóm transport cần cung cấp
client connect/receive/send/close tương thích nếu muốn chuyển đường peer sang
`streaprous_socket`. Raw socket hiện có chủ yếu là phía server, chưa có client
connect, chưa xác nhận ACK/retransmission, ghép nhiều segment và đóng an toàn.
Đường HTTP raw cần được kiểm thử trên VM có quyền raw socket trước khi demo.

Khi có proxy, địa chỉ tracker nhìn thấy có thể là IP proxy. Cần thống nhất
cách truyền IP peer đáng tin cậy và tích hợp authentication; code hiện chưa xác
thực quyền sở hữu peer_id. NAT/firewall cũng có thể làm endpoint không truy cập được.
