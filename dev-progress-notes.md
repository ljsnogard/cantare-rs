# smux v1 握手协议

## 握手发起方
1. 构造并发送 Invitation
2. 等待回复
3. 如果回应 K_ACCEPT_MAGIC 且返回的协商条件一致，则认为握手成功
4. 若握手成功，发送 K_CONFRM_MAGIC 以及完整协商结果
5. 等待对方发送 K_CONFRM_MAGIC

## 等待握手放
1. 等待并接收 Invitation，并对 Invitation 内容进行 CRC 校验
2. 发送 K_ACCEPT_MAGIC 并重复握手发起方的协商条件，补全所有未提及的基础协商值
3. 等待对方确认协商结果，并判断是否与补全的基础协商值一致。
4. 如果确认则发送 K_CONFRM 但不再带有协商条件；否则发送 K_REJECT_MAGIC
