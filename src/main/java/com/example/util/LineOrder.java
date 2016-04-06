package com.example.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.example.message.ManagedQueueMessage;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LineOrder {
    List<String> before = new ArrayList<>();
    List<String> after = new ArrayList<>();
    Optional<ManagedQueueMessage> modifiedMessageOpt = Optional.empty();

    public boolean isModified() {
        return modifiedMessageOpt.isPresent();
    }
}
