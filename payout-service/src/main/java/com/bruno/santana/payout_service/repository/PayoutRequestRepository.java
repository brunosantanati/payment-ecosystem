package com.bruno.santana.payout_service.repository;

import com.bruno.santana.payout_service.domain.PayoutRequest;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PayoutRequestRepository extends JpaRepository<PayoutRequest, String> {
}